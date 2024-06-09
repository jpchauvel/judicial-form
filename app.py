#!/usr/bin/env python3
import asyncio
import logging
import tracemalloc
from functools import wraps

import typer
from rich.progress import track
from typing_extensions import Annotated

from conf import Settings, get_settings
from expressvpn import AsyncExpressVpnApi
from scraper import scraper
from util import get_year, write_header_to_csv
from worker import add_worker, sync_workers

# Configure the root logger
logger: logging.Logger = logging.getLogger(__name__)

# Configure the asyncio logger
asyncio_logger: logging.Logger = logging.getLogger("asyncio")
asyncio_logger.setLevel(logging.WARNING)

# Configure the httpx logger
httpx_logger: logging.Logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)


def typer_async(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


async def progress_bar(event: asyncio.Event, steps: int) -> None:
    for _ in track(range(steps)):
        await event.wait()
        event.clear()


async def memory_profiler(logger: logging.Logger) -> None:
    settings: Settings = get_settings()
    tracemalloc.start()
    while True:
        await asyncio.sleep(settings.memory_profiler_frequency_in_seconds)
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")
        for stat in top_stats[:10]:
            logger.info(stat)


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
    disable_progress_bar: Annotated[
        bool, typer.Option(help="Disable progress bar")
    ] = False,
) -> None:
    settings: Settings = get_settings()
    tasks: list[asyncio.Task[None]] = []

    if settings.with_memory_profiler:
        # Start memory profiler
        memory_profiler_task: asyncio.Task[None] = asyncio.create_task(
            coro=memory_profiler(logger=logger)
        )
        tasks.append(memory_profiler_task)

    await write_header_to_csv(output_file)
    # Configure the root logger's level
    logging.basicConfig(
        level=logging.DEBUG if settings.debug else logging.INFO
    )

    now: int = get_year(until_year)
    input_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
    sync_workers_event: asyncio.Event = asyncio.Event()
    progress_bar_event: asyncio.Event = asyncio.Event()

    for year in range(settings.since, now + 1):
        for i in range(document_range):
            input_queue.put_nowait((str(document_number_start + i), str(year)))

    async with AsyncExpressVpnApi(logger=logger) as vpn_api:
        await vpn_api.rotate_vpn()  # First rotation

        # Start workers
        for _ in range(num_workers):
            worker_task: asyncio.Task[None] = add_worker(
                input_queue=input_queue,
                sync_workers_event=sync_workers_event,
                progress_bar_event=progress_bar_event,
                output_file=output_file,
                logger=logger,
                scraper=scraper,
            )
            tasks.append(worker_task)

        # Start the worker synchronizer
        sync_workers_task: asyncio.Task[None] = asyncio.create_task(
            coro=sync_workers(
                input_queue=input_queue,
                vpn_api=vpn_api,
                sync_workers_event=sync_workers_event,
                logger=logger,
            )
        )
        tasks.append(sync_workers_task)

        if not disable_progress_bar:
            # Start progress bar
            progress_bar_task: asyncio.Task[None] = asyncio.create_task(
                coro=progress_bar(
                    event=progress_bar_event,
                    steps=document_range * (now - settings.since + 1),
                )
            )
            tasks.append(progress_bar_task)

        # Wait until the queue is fully processed
        await input_queue.join()

        for task in tasks:
            task.cancel()

        # Wait until all tasks are cancelled
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    typer.run(main)
