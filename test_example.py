#!/usr/bin/env python3
import asyncio
from pathlib import Path
import random
import string

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    async_playwright,
)
from playwright_stealth import stealth_async
from pydantic_settings import BaseSettings, SettingsConfigDict
from unicaps import AsyncCaptchaSolver, CaptchaSolvingService


class Settings(BaseSettings):
    anti_captcha_api_key: str = ""
    url: str = ""

    model_config = SettingsConfigDict(env_file=".env")


def generate_random_string():
    characters = string.ascii_lowercase + string.digits
    return "".join(random.choice(characters) for _ in range(10))


async def main() -> None:
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

        await page.locator("#anio").select_option("2020")

        await page.locator("#numeroExpediente").fill("1000")

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

        # Take a screenshot
        screenshot_filename: str = "pj.png"
        await page.screenshot(path=screenshot_filename)

        # Close the browser
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
