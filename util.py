from datetime import datetime

import aiocsv
import aiofiles

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


def get_year(year: str) -> int:
    if year == "current":
        return datetime.now().year
    return int(year)
