#!/usr/bin/env python3
import asyncio
from datetime import datetime
from functools import lru_cache, wraps
import math
from pathlib import Path

import aiocsv
import aiofiles
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    TimeoutError,
    async_playwright,
)
from playwright_stealth import stealth_async
from pydantic_settings import BaseSettings, SettingsConfigDict
from rand_useragent import randua
from rich.progress import track
import typer
from typing_extensions import Annotated
from undetected_playwright import Malenia
from unicaps import AsyncCaptchaSolver, CaptchaSolvingService

FIELDS = [
    "document_number",
    "court",
    "judge",
    "date_start",
    "subject",
    "state",
    "plaintiff",
    "defendant",
]

workers: list[asyncio.Task[None]] = []


class Settings(BaseSettings):
    anti_captcha_api_key_path: str = ""
    url: str = ""
    since: int = 0
    threshold: float = .0

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


@lru_cache
def get_settings() -> Settings:
    return Settings()


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
    return {FIELDS[i]: value for i, value in enumerate(data)}


async def write_header_to_csv(csv_file) -> None:
    async with aiofiles.open(csv_file, "w", newline="") as csvfile:
        writer: aiocsv.AsyncDictWriter = aiocsv.AsyncDictWriter(
            csvfile, fieldnames=FIELDS
        )
        await writer.writeheader()


async def save_dict_to_csv(row: dict[str, str], csv_file) -> None:
    if not row:
        return

    async with aiofiles.open(csv_file, "a", newline="") as csvfile:
        writer: aiocsv.AsyncDictWriter = aiocsv.AsyncDictWriter(
            csvfile,
            fieldnames=FIELDS,
        )
        await writer.writerow(row)


async def worker(
    incoming_queue: asyncio.Queue[tuple[str, str, str]],
    cancelled_workers_queue: asyncio.Queue[bool],
    progress_bar_queue: asyncio.Queue[bool],
) -> None:
    while True:
        document_number, year, output_file = await incoming_queue.get()
        settings: Settings = get_settings()
        async with async_playwright() as p:
            browser: Browser = await p.firefox.launch(headless=False)
            try:
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

                    await asyncio.sleep(1)

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

                            await asyncio.sleep(1)

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

                            await asyncio.sleep(1)

                            anchors: list = await page.locator(
                                "div#divCuerpo a"
                            ).all()
                            await anchors[0].click()

                            await asyncio.sleep(1)

                            # Wait for the webpage to load completely
                            await page.wait_for_load_state("load")

                            buttons: list = await page.locator(
                                "div#divDetalles button"
                            ).all()
                            buttons_length: int = len(buttons)
                            i += 1

                    await progress_bar_queue.put(True)
                    incoming_queue.task_done()

                except TimeoutError:
                    # Close the browser
                    await browser.close()
                    await cancelled_workers_queue.put(True)
                    await asyncio.sleep(.5)
            finally:
                if browser.is_connected():
                    # Close the browser
                    await browser.close()


async def progress_bar(queue: asyncio.Queue[bool], steps: int) -> None:
    for _ in track(range(steps)):
        await queue.get()
        queue.task_done()


async def respawn_workers(
    vpn_settings: dict,
    num_workers: int,
    incoming_queue: asyncio.Queue[tuple[str, str, str]],
    cancelled_workers_queue: asyncio.Queue[bool],
    progress_bar_queue: asyncio.Queue[bool],
) -> None:
    global workers
    settings: Settings = get_settings()
    count: int = num_workers
    threshold: int = math.ceil(num_workers * settings.threshold)
    while True:
        while count > threshold:
            await cancelled_workers_queue.get()
            count -= 1
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        rotate_VPN(vpn_settings)
        workers = []
        for _ in range(num_workers):
            worker_task = create_worker(
                incoming_queue=incoming_queue,
                cancelled_workers_queue=cancelled_workers_queue,
                progress_bar_queue=progress_bar_queue,
            )
            workers.append(worker_task)


def create_worker(
    incoming_queue: asyncio.Queue[tuple[str, str, str]],
    cancelled_workers_queue: asyncio.Queue[bool],
    progress_bar_queue: asyncio.Queue[bool],
) -> asyncio.Task[None]:
    return asyncio.create_task(
        coro=worker(
            incoming_queue=incoming_queue,
            cancelled_workers_queue=cancelled_workers_queue,
            progress_bar_queue=progress_bar_queue,
        )
    )


def get_year(year: str) -> int:
    if year == "current":
        return datetime.now().year
    return int(year)


@typer_async
async def main(
    document_number_start: Annotated[
        int, typer.Option(help="The document start number")
    ],
    document_range: Annotated[int, typer.Option(help="The document range")],
    until_year: Annotated[
        str, typer.Option(help="Process until this year")
    ] = "current",
    output_file: Annotated[
        str, typer.Option(help="The output file")
    ] = "output.csv",
    num_workers: Annotated[
        int, typer.Option(help="The number of workers")
    ] = 5,
) -> None:
    global workers
    vpn_settings = initialize_VPN(
        stored_settings=0,
        save=0,
        area_input=["Chile"],
        skip_settings=1,
    )
    rotate_VPN(vpn_settings)
    await write_header_to_csv(output_file)
    settings: Settings = get_settings()

    now = get_year(until_year)
    incoming_queue: asyncio.Queue[tuple[str, str, str]] = asyncio.Queue()
    progress_bar_queue: asyncio.Queue[bool] = asyncio.Queue()
    cancelled_workers_queue: asyncio.Queue[bool] = asyncio.Queue()

    for year in range(settings.since, now + 1):
        for i in range(document_range):
            incoming_queue.put_nowait(
                (str(document_number_start + i), str(year), output_file)
            )

    tasks: list[asyncio.Task[None]] = []

    # Start the respawn workers task
    respawn_workers_task: asyncio.Task[None] = asyncio.create_task(
        coro=respawn_workers(
            vpn_settings=vpn_settings,
            num_workers=num_workers,
            incoming_queue=incoming_queue,
            cancelled_workers_queue=cancelled_workers_queue,
            progress_bar_queue=progress_bar_queue,
        )
    )
    tasks.append(respawn_workers_task)

    # Start workers
    for _ in range(num_workers):
        worker_task: asyncio.Task[None] = create_worker(
            incoming_queue=incoming_queue,
            cancelled_workers_queue=cancelled_workers_queue,
            progress_bar_queue=progress_bar_queue,
        )
        workers.append(worker_task)
        tasks.append(worker_task)

    # Start progress bar
    progress_bar_task: asyncio.Task[None] = asyncio.create_task(
        coro=progress_bar(
            queue=progress_bar_queue,
            steps=document_range * (now - settings.since + 1),
        )
    )
    tasks.append(progress_bar_task)

    # Wait until the queues are fully processed
    await incoming_queue.join()
    await cancelled_workers_queue.join()
    await progress_bar_queue.join()

    for task in tasks:
        task.cancel()

    # Wait until all tasks are cancelled
    await asyncio.gather(*tasks, return_exceptions=True)
    terminate_VPN(vpn_settings)


if __name__ == "__main__":
    typer.run(main)
