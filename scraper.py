import asyncio
from pathlib import Path

import aiofiles
from playwright.async_api import Browser, BrowserContext, Page, expect
from playwright_stealth import stealth_async
from rand_useragent import randua
from undetected_playwright import Malenia
from unicaps import AsyncCaptchaSolver, CaptchaSolvingService

from conf import Settings, get_settings
from util import (clean_header, clean_parties, convert_data_to_dict,
                  save_dict_to_csv)


async def scraper(
    browser: Browser, output_file: str, *input_values: str
) -> None:
    settings: Settings = get_settings()

    document_number, year = input_values

    # Create  a new context and page
    context: BrowserContext = await browser.new_context(user_agent=randua())

    try:
        await Malenia.apply_stealth(context)

        page: Page = await context.new_page()

        # Apply the stealth settings
        await stealth_async(page)

        # Navigate to the page
        await page.goto(settings.url)

        page.set_default_timeout(settings.timeout)

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

        passed = False
        while not passed:
            async with aiofiles.tempfile.NamedTemporaryFile() as tmpfile:
                await page.locator("#captcha_image").screenshot(
                    path=tmpfile.name
                )
                async with AsyncCaptchaSolver(
                    CaptchaSolvingService.ANTI_CAPTCHA,
                    settings.anti_captcha_api_key,
                ) as solver:
                    solved = await solver.solve_image_captcha(
                        Path(tmpfile.name),
                        is_phrase=False,
                        is_case_sensitive=False,
                    )
                    await page.locator("#codigoCaptcha").fill(
                        solved.solution.text
                    )

                    await page.locator("#consultarExpedientes").click()

                    try:
                        await expect(
                            page.get_by_text(
                                "Ingrese el Codigo de Captcha Correcto"
                            )
                        ).to_be_visible()
                    except AssertionError:
                        await solved.report_good()
                        passed = True
                    else:
                        passed = False

        await asyncio.sleep(settings.delay_after_click)

        # Wait for the webpage to load completely
        await page.wait_for_load_state("load")

        buttons: list = await page.locator("div#divDetalles button").all()
        buttons_length: int = len(buttons)
        i: int = 0
        while i < buttons_length:
            if (
                await buttons[i].get_attribute("title")
                == "Ver detalle de expediente"
            ):
                await buttons[i].click()

                await asyncio.sleep(settings.delay_after_click)

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
                    await save_dict_to_csv(converted_data, output_file)

                anchors: list = await page.locator("div#divCuerpo a").all()
                await anchors[0].click()

                await asyncio.sleep(settings.delay_after_click)

                # Wait for the webpage to load completely
                await page.wait_for_load_state("load")

                buttons: list = await page.locator(
                    "div#divDetalles button"
                ).all()
                buttons_length: int = len(buttons)
                i += 1
    finally:
        await context.close()
