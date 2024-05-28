#!/usr/bin/env python3
import asyncio
import csv
from functools import wraps
from pathlib import Path
import random
import re
import string

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    async_playwright,
)
from playwright_stealth import stealth_async
from pydantic_settings import BaseSettings, SettingsConfigDict
import typer
from typing_extensions import Annotated
from unicaps import AsyncCaptchaSolver, CaptchaSolvingService

plaintiff_re: re.Pattern[str] = re.compile(r".*DEMANDANTE:\s*([^.]*).*\.")
defendat_re: re.Pattern[str] = re.compile(r".*DEMANDADO:\s*([^.]*).*\.")


class Settings(BaseSettings):
    anti_captcha_api_key: str = ""
    url: str = ""

    model_config = SettingsConfigDict(env_file=".env")


def generate_random_string():
    characters = string.ascii_lowercase + string.digits
    return "".join(random.choice(characters) for _ in range(10))


def clean_parties(parties: list[str]) -> list[str]:
    return [party.strip() for party in parties]


def convert_parties_to_dict_list(parties: list[str]) -> list[dict]:
    return [
        {
            "plaintiff": plaintiff_re.match(party).group(1).strip(),
            "defendant": defendat_re.match(party).group(1).strip()
            if defendat_re.match(party) is not None
            else "",
        }
        for party in parties
    ]


def typer_async(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def save_dict_list_to_csv(dict_list, csv_file):
    if not dict_list:
        return

    keys = dict_list[0].keys()

    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        writer.writerows(dict_list)


@typer_async
async def main(
    year: Annotated[str, typer.Option(help="The year of the document")],
    document_number: Annotated[str, typer.Option(help="The document number")],
) -> None:
    settings: Settings = Settings()
    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch()

        # Create  a new context and page
        context: BrowserContext = await browser.new_context(
            user_agent=generate_random_string()
        )
        page: Page = await context.new_page()

        # Apply the stealth settings
        await stealth_async(page)

        # Navigate to the page
        await page.goto(settings.url)

        # Wait for the webpage to load completely
        await page.wait_for_load_state("load")

        await page.locator("#distritoJudicial").select_option("LIMA")
        await page.locator("#distritoJudicial").dispatch_event("change")

        await page.wait_for_function(
            """
            document.querySelector('#organoJurisdiccional option[value="16133"]')
        """
        )
        await page.locator("#organoJurisdiccional").select_option(
            "JUZGADO DE PAZ LETRADO"
        )
        await page.locator("#organoJurisdiccional").dispatch_event("change")

        await page.wait_for_function(
            """
            document.querySelector('#especialidad option[value="97880"]')
        """
        )
        await page.locator("#especialidad").select_option("CIVIL")

        await page.locator("#anio").select_option(year)

        await page.locator("#numeroExpediente").fill(document_number)

        await page.wait_for_selector("#captcha_image")
        await page.locator("#captcha_image").screenshot(path="captcha.png")

        async with AsyncCaptchaSolver(
            CaptchaSolvingService.ANTI_CAPTCHA, settings.anti_captcha_api_key
        ) as solver:
            solved = await solver.solve_image_captcha(
                Path("captcha.png"),
                is_phrase=False,
                is_case_sensitive=False,
            )
            await page.locator("#codigoCaptcha").fill(solved.solution.text)
            await solved.report_good()

        await page.locator("#consultarExpedientes").click()

        await asyncio.sleep(2)

        # Wait for the webpage to load completely
        await page.wait_for_load_state("load")

        parties: list[str] = await page.evaluate(
            "Array.from(document.getElementsByName('partesp'), element => element.textContent)"
        )
        if len(parties) == 0:
            parties: list[str] = await page.evaluate(
                "Array.from(document.getElementsByClassName('partesp'), element => element.textContent)"
            )

        parties: list[str] = clean_parties(parties)

        parties_dicts: list[dict] = convert_parties_to_dict_list(parties)

        save_dict_list_to_csv(parties_dicts, "parties.csv")

        # Take a screenshot
        screenshot_filename: str = "pj.png"
        await page.screenshot(path=screenshot_filename)

        # Close the browser
        await browser.close()


if __name__ == "__main__":
    typer.run(main)
