#!/usr/bin/env python3
import asyncio
import csv
from functools import wraps
from pathlib import Path

from rand_useragent import randua
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
from undetected_playwright import Malenia


class Settings(BaseSettings):
    anti_captcha_api_key_path: str = ""
    url: str = ""

    model_config = SettingsConfigDict(env_file=".env")

    def __init__(self, **data) -> None:
        super().__init__(**data)
        self._anti_captcha_api_key = (
            Path(self.anti_captcha_api_key_path)
            .expanduser()
            .read_text()
            .strip()
        )

    @property
    def anti_captcha_api_key(self) -> str:
        return self._anti_captcha_api_key


def typer_async(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def clean_header(data: str) -> list[str]:
    row_to_list: list[str] = []
    for item in data.split("\n"):
        item_stripped: str = item.strip()
        if item_stripped == "":
            continue
        row_to_list.append(item_stripped)
    return [
        row_to_list[1],  # document_number
        row_to_list[3],  # court
        row_to_list[7],  # judge
        row_to_list[11],  # date_start
        row_to_list[19],  # subject
        row_to_list[21],  # state
    ]


def clean_parties(data: str) -> list[str]:
    row_to_list: list[str] = []
    for item in data.split("\n"):
        item_stripped: str = item.strip()
        if item_stripped == "":
            continue
        row_to_list.append(item_stripped)
    return [
        row_to_list[7],  # plaintiff
        row_to_list[10] if len(row_to_list) > 10 else "",  # defendant
    ]


def convert_data_to_dict(data: list[str]) -> dict[str, str]:
    return {
        "document_number": data[0],
        "court": data[1],
        "judge": data[2],
        "date_start": data[3],
        "subject": data[4],
        "state": data[5],
        "plaintiff": data[6],
        "defendant": data[7],
    }


def save_dict_to_csv(
    row: dict[str, str], csv_file, write_header: bool = False
) -> None:
    if not row:
        return

    with open(csv_file, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=row.keys())
        if write_header:
            writer.writeheader()
        writer.writerow(row)


@typer_async
async def main(
    year: Annotated[str, typer.Option(help="The year of the document")],
    document_number: Annotated[str, typer.Option(help="The document number")],
    output_file: Annotated[
        str, typer.Option(help="The output file")
    ] = "output.csv",
) -> None:
    settings: Settings = Settings()
    async with async_playwright() as p:
        browser: Browser = await p.firefox.launch()

        # Create  a new context and page
        context: BrowserContext = await browser.new_context(
            user_agent=randua()
        )
        await Malenia.apply_stealth(context)

        page: Page = await context.new_page()

        # Apply the stealth settings
        await stealth_async(page)

        # Navigate to the page
        await page.goto(settings.url)

        # Wait for the webpage to load completely
        await page.wait_for_load_state("load")

        await page.locator("#distritoJudicial").select_option("LIMA")

        await page.wait_for_function(
            """
            document.querySelector('#organoJurisdiccional option[value="16133"]')
        """
        )
        await page.locator("#organoJurisdiccional").select_option(
            "JUZGADO DE PAZ LETRADO"
        )

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

        buttons: list = await page.locator("div#divDetalles button").all()
        buttons_length = len(buttons)
        i: int = 0
        while i < buttons_length:
            if (
                await buttons[i].get_attribute("title")
                == "Ver detalle de expediente"
            ):
                await buttons[i].click()
                await asyncio.sleep(2)
                # Wait for the webpage to load completely
                await page.wait_for_load_state("load")

                header_content: str | None = await page.locator(
                    "div#gridRE"
                ).text_content()
                parties_content: str | None = await page.locator(
                    "div#collapseTwo"
                ).text_content()
                if header_content is not None and parties_content is not None:
                    cleaned_header: list[str] = clean_header(header_content)
                    cleaned_parties: list[str] = clean_parties(parties_content)
                    data: list[str] = cleaned_header + cleaned_parties
                    converted_data: dict[str, str] = convert_data_to_dict(data)
                    save_dict_to_csv(converted_data, output_file, i == 0)

                await asyncio.sleep(2)

                anchors: list = await page.locator("div#divCuerpo a").all()
                await anchors[0].click()

                await asyncio.sleep(2)

                # Wait for the webpage to load completely
                await page.wait_for_load_state("load")
                buttons: list = await page.locator(
                    "div#divDetalles button"
                ).all()
                i += 1

        ## Take a screenshot
        #screenshot_filename: str = "pj.png"
        #await page.screenshot(path=screenshot_filename)

        # Close the browser
        await browser.close()


if __name__ == "__main__":
    typer.run(main)
