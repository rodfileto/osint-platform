from __future__ import annotations

import csv
from io import BytesIO, StringIO

from django.core.files.base import ContentFile
from django.core.files.storage import storages
from openpyxl import Workbook


def _serialize_cell(value):
    if value is None:
        return ''
    if isinstance(value, (list, tuple, set)):
        return ', '.join(str(item) for item in value)
    return str(value)


def build_export_bytes(rows: list[dict], field_names: list[str], export_format: str) -> tuple[bytes, str]:
    if export_format == 'csv':
        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=field_names)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _serialize_cell(row.get(field)) for field in field_names})
        return buffer.getvalue().encode('utf-8'), 'text/csv'

    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = 'cnpj_search'
    worksheet.append(field_names)
    for row in rows:
        worksheet.append([_serialize_cell(row.get(field)) for field in field_names])

    output = BytesIO()
    workbook.save(output)
    return (
        output.getvalue(),
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )


def persist_export(storage_name: str, object_key: str, content: bytes) -> str:
    storage = storages[storage_name]
    return storage.save(object_key, ContentFile(content))


def open_export(storage_name: str, object_key: str):
    storage = storages[storage_name]
    return storage.open(object_key, 'rb')
