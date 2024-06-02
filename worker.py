import asyncio
import gc
import logging
import math
from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

from playwright.async_api import Browser, async_playwright, TimeoutError

from conf import Settings, get_settings
from expressvpn import AsyncExpressVpnApi

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
    logger: logging.Logger,
) -> None:
    logger.debug("Worker synchronizer started.")
    settings: Settings = get_settings()
    count: int = get_num_workers()
    threshold: int = math.ceil(count * settings.threshold)
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
        logger.debug(
            f"Threshold reached."
            f" Total workers remaining: {get_num_workers()}."
            f" Threshold: {threshold}."
        )
        count: int = get_num_workers()
        threshold: int = math.ceil(count * settings.threshold)
        await vpn_api.rotate_vpn()  # Rotate VPN


async def worker(
    input_queue: asyncio.Queue[tuple[str, str]],
    cancelled_workers_queue: asyncio.Queue[bool],
    sync_workers_event: asyncio.Event,
    progress_bar_event: asyncio.Event,
    output_file: str,
    logger: logging.Logger,
    scraper: Callable[..., Awaitable[None]],
) -> None:
    input_values: tuple[str, str] | None = None
    browser: Browser | None = None
    task_done: bool | None = None
    worker_id: UUID = uuid4()
    logger.debug(f"Worker {worker_id} started.")
    try:
        settings: Settings = get_settings()
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=settings.headless)
            while True:
                try:
                    task_done = False
                    await sync_workers_event.wait()  # Wait for syncrhonization event
                    input_values = await input_queue.get()

                    await scraper(browser, output_file, *input_values)

                    logger.debug(
                        f"Worker {worker_id} finished processing"
                        f" {[x for x in input_values]}."
                    )
                    progress_bar_event.set()
                    input_queue.task_done()
                    task_done = True

                except TimeoutError:
                    logger.debug(f"Worker {worker_id} timed out.")
                    if input_values is not None:
                        input_queue.task_done()
                        await input_queue.put(input_values)
                        await cancelled_workers_queue.put(True)

                except Exception as err:
                    # FIXME: Catchall exception
                    logger.debug(
                        f"Worker {worker_id} ended with an exception."
                    )
                    logger.debug("Exception: ", exc_info=err)
                    if input_values is not None:
                        input_queue.task_done()
                        await input_queue.put(input_values)
                        await cancelled_workers_queue.put(True)
                finally:
                    if settings.with_garbage_collection:
                        logger.debug("Garbage collection initiated...")
                        gc.collect()

    except asyncio.CancelledError:
        logger.debug(f"Worker {worker_id} was cancelled.")
        if (
            task_done is not None
            and not task_done
            and input_values is not None
        ):
            input_queue.task_done()
            await input_queue.put(input_values)
            await cancelled_workers_queue.put(True)
        # Close the browser
        if browser is not None and browser.is_connected():
            await browser.close()
        decrease_num_workers()
        raise asyncio.CancelledError
