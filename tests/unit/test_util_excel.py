import pytest
from openpyxl import Workbook
from tulona.util.excel import get_column_index


def _create_dummy_sheet():
    wb = Workbook()
    ws = wb.active
    ws.title = "DummySheet"

    for row in range(1, 6):
        for col in range(1, 6):
            ws.cell(row=row, column=col, value=f"Row{row} Col{col}")

    return wb


@pytest.mark.parametrize(
    "sheet,column,expected",
    [
        (_create_dummy_sheet()["DummySheet"], "Row1 Col2", 2),
        pytest.param(
            _create_dummy_sheet()["DummySheet"],
            "Row1 Col10",
            2,
            marks=pytest.mark.xfail(
                raises=ValueError, match="could not be found in the Excel sheet"
            ),
        ),
    ],
)
def test_get_column_index(sheet, column, expected):
    actual = get_column_index(sheet, column)
    assert actual == expected
