import asyncio
import gc
import logging
import math
from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

from playwright.async_api import Browser, TimeoutError, async_playwright

from conf import Settings, get_settings
from expressvpn import AsyncExpressVpnApi

_workers: dict[UUID, tuple[bool, BaseException | None]] = {}


def get_num_workers() -> int:
    global _workers
    return len(_workers)


def remove_worker(worker_id: UUID) -> None:
    global _workers
    del _workers[worker_id]


def reset_worker(worker_id: UUID) -> None:
    global _workers
    _workers[worker_id] = (False, None)


def set_worker_exception(worker_id: UUID, exception: BaseException) -> None:
    global _workers
    _workers[worker_id] = (False, exception)


def set_worker_done(worker_id: UUID) -> None:
    global _workers
    _workers[worker_id] = (True, None)


def add_worker(
    input_queue: asyncio.Queue[tuple[str, str]],
    sync_workers_event: asyncio.Event,
    progress_bar_event: asyncio.Event,
    output_file: str,
    logger: logging.Logger,
    scraper: Callable[..., Awaitable[None]],
) -> asyncio.Task[None]:
    global _workers
    worker_id: UUID = uuid4()
    reset_worker(worker_id)
    task: asyncio.Task[None] = asyncio.create_task(
        coro=worker(
            worker_id=worker_id,
            input_queue=input_queue,
            sync_workers_event=sync_workers_event,
            progress_bar_event=progress_bar_event,
            output_file=output_file,
            logger=logger,
            scraper=scraper,
        )
    )
    return task


async def sync_workers(
    vpn_api: AsyncExpressVpnApi,
    input_queue: asyncio.Queue[tuple[str, str]],
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
            for task_done, err in _workers.values():
                if not task_done and err is not None:
                    count -= 1
            await asyncio.sleep(0.1)
        pending_tasks: bool = True
        while pending_tasks and not input_queue.empty():
            pending_tasks = False
            for task_done, err in _workers.values():
                if not task_done and err is None:
                    pending_tasks = True
                    break
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
    worker_id: UUID,
    input_queue: asyncio.Queue[tuple[str, str]],
    sync_workers_event: asyncio.Event,
    progress_bar_event: asyncio.Event,
    output_file: str,
    logger: logging.Logger,
    scraper: Callable[..., Awaitable[None]],
) -> None:
    input_values: tuple[str, str] | None = None
    browser: Browser | None = None
    logger.debug(f"Worker {worker_id} started.")
    try:
        settings: Settings = get_settings()
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=settings.headless)
            while True:
                try:
                    await sync_workers_event.wait()  # Wait for syncrhonization event
                    input_values = await input_queue.get()
                    reset_worker(worker_id)

                    await scraper(browser, output_file, *input_values)

                    logger.debug(
                        f"Worker {worker_id} finished processing"
                        f" {[x for x in input_values]}."
                    )
                    progress_bar_event.set()
                    set_worker_done(worker_id)

                except TimeoutError as err:
                    logger.debug(f"Worker {worker_id} timed out.")
                    if input_values is not None:
                        set_worker_exception(worker_id, err)
                        await input_queue.put(input_values)

                except Exception as err:
                    # FIXME: Catchall exception
                    logger.debug(
                        f"Worker {worker_id} ended with an exception."
                    )
                    logger.debug("Exception: ", exc_info=err)
                    if input_values is not None:
                        set_worker_exception(worker_id, err)
                        await input_queue.put(input_values)
                finally:
                    input_queue.task_done()
                    if settings.with_garbage_collection:
                        logger.debug("Garbage collection initiated...")
                        gc.collect()

    except asyncio.CancelledError as err:
        logger.debug(f"Worker {worker_id} was cancelled.")
        # Close the browser
        if browser is not None and browser.is_connected():
            await browser.close()
        remove_worker(worker_id)
        raise asyncio.CancelledError
