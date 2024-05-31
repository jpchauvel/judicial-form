import asyncio
import math
from pathlib import Path

import aiofiles
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    TimeoutError,
    async_playwright,
)
from playwright_stealth import stealth_async
from rand_useragent import randua
from undetected_playwright import Malenia
from unicaps import AsyncCaptchaSolver, CaptchaSolvingService

from conf import Settings, get_settings
from expressvpn import AsyncExpressVpnApi
from util import (
    clean_header,
    clean_parties,
    convert_data_to_dict,
    save_dict_to_csv,
) 

_num_workers: int = 0


def set_num_workers(num_workers: int) -> None:
    global _num_workers
    _num_workers = num_workers


def get_num_workers() -> int:
    global _num_workers
    return _num_workers


def decrease_num_workers() -> None:
    global _num_workers
    _num_workers -= 1


async def sync_workers(
    vpn_api: AsyncExpressVpnApi,
    cancelled_workers_queue: asyncio.Queue[bool],
    sync_workers_event: asyncio.Event,
) -> None:
    settings: Settings = get_settings()
    count: int = get_num_workers()
    threshold: int = math.ceil(count* settings.threshold)
    while True:
        while count >= threshold:
            sync_workers_event.set()
            sync_workers_event.clear()
            try:
                if cancelled_workers_queue.get_nowait():
                    count -= 1
                    cancelled_workers_queue.task_done()
            except asyncio.QueueEmpty:
                pass
            await asyncio.sleep(0.1)
        count: int = get_num_workers()
        threshold: int = math.ceil(count* settings.threshold)
        await vpn_api.rotate_vpn()  # Rotate VPN


async def worker(
    input_queue: asyncio.Queue[tuple[str, str]],
    cancelled_workers_queue: asyncio.Queue[bool],
    sync_workers_event: asyncio.Event,
    progress_bar_event: asyncio.Event,
    output_file: str,
) -> None:
    document_number: str | None = None
    year: str | None = None
    browser: Browser | None = None
    task_done: bool | None = None
    try:
        async with async_playwright() as p:
            settings: Settings = get_settings()
            browser = await p.firefox.launch(headless=settings.headless)
            while True:
                try:
                    task_done = False
                    await sync_workers_event.wait()  # Wait for syncrhonization event
                    document_number, year = await input_queue.get()
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

                    page.set_default_timeout(settings.timeout)

                    try:
                        # Wait for the webpage to load completely
                        await page.wait_for_load_state("load")

                        await page.locator("#distritoJudicial").select_option(
                            "LIMA"
                        )

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

                        await page.locator("#numeroExpediente").fill(
                            document_number
                        )

                        await page.wait_for_selector("#captcha_image")

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
                                await solved.report_good()

                        await page.locator("#consultarExpedientes").click()

                        await asyncio.sleep(settings.delay_after_click)

                        # Wait for the webpage to load completely
                        await page.wait_for_load_state("load")

                        buttons: list = await page.locator(
                            "div#divDetalles button"
                        ).all()
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

                                if (
                                    header_content is not None
                                    and parties_content is not None
                                ):
                                    cleaned_header: list[str] = clean_header(
                                        header_content
                                    )
                                    cleaned_parties: list[str] = clean_parties(
                                        parties_content
                                    )
                                    data: list[str] = (
                                        cleaned_header + cleaned_parties
                                    )
                                    converted_data: dict[
                                        str, str
                                    ] = convert_data_to_dict(data)
                                    await save_dict_to_csv(
                                        converted_data, output_file
                                    )

                                anchors: list = await page.locator(
                                    "div#divCuerpo a"
                                ).all()
                                await anchors[0].click()

                                await asyncio.sleep(settings.delay_after_click)

                                # Wait for the webpage to load completely
                                await page.wait_for_load_state("load")

                                buttons: list = await page.locator(
                                    "div#divDetalles button"
                                ).all()
                                buttons_length: int = len(buttons)
                                i += 1

                        progress_bar_event.set()
                        input_queue.task_done()
                        task_done = True

                    except TimeoutError:
                        if document_number is not None and year is not None:
                            input_queue.task_done()
                            await input_queue.put((document_number, year))
                            await cancelled_workers_queue.put(True)

                except Exception:
                    # FIXME: Catchall exception
                    if document_number is not None and year is not None:
                        input_queue.task_done()
                        await input_queue.put((document_number, year))
                        await cancelled_workers_queue.put(True)

    except asyncio.CancelledError:
        if task_done is not None and not task_done and document_number is not None and year is not None:
            input_queue.task_done()
            await input_queue.put((document_number, year))
            await cancelled_workers_queue.put(True)
        # Close the browser
        if browser is not None and browser.is_connected():
            await browser.close()
        decrease_num_workers()
        raise asyncio.CancelledError
