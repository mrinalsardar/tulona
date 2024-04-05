from typing import Union

from openpyxl import load_workbook, styles
from openpyxl.styles import Border, Side
from openpyxl.worksheet.worksheet import Worksheet


def get_column_index(sheet: Worksheet, column: str):
    for row in sheet.iter_rows(min_row=1, max_row=1, values_only=True):
        for idx, cell_value in enumerate(row, 1):
            if cell_value == column:
                return idx
    raise ValueError(f"Column {column} could not be found in the Excel sheet.")


def highlight_mismatch_pair(excel_file, sheet):
    wb = load_workbook(excel_file)
    ws = wb[sheet]

    yellow_fill = styles.PatternFill(
        start_color="FFFFFF00", end_color="FFFFFF00", fill_type="solid"
    )

    left_border = Border(
        left=Side(border_style="thin"),
        right=Side(border_style="dotted"),
        top=Side(border_style="thin"),
        bottom=Side(border_style="thin"),
    )
    right_border = Border(
        left=Side(border_style="dotted"),
        right=Side(border_style="thin"),
        top=Side(border_style="thin"),
        bottom=Side(border_style="thin"),
    )

    for row in ws.iter_rows(
        min_row=2, min_col=0, max_row=ws.max_row, max_col=ws.max_column
    ):
        for col_idx in range(0, ws.max_column, 2):
            if row[col_idx].value != row[col_idx + 1].value:
                row[col_idx].fill = yellow_fill
                row[col_idx + 1].fill = yellow_fill

                row[col_idx].border = left_border
                row[col_idx + 1].border = right_border

    wb.save(excel_file)


def highlight_mismatch_cells(
    excel_file, sheet: str, num_ds: int, skip_columns: Union[str, list[str]] = None
):
    wb = load_workbook(excel_file)
    ws = wb[sheet]

    yellow_fill = styles.PatternFill(
        start_color="FFFFFF00", end_color="FFFFFF00", fill_type="solid"
    )

    left_border = Border(
        left=Side(border_style="thin"),
        right=Side(border_style="dotted"),
        top=Side(border_style="thin"),
        bottom=Side(border_style="thin"),
    )

    middle_border = Border(
        left=Side(border_style="dotted"),
        right=Side(border_style="dotted"),
        top=Side(border_style="thin"),
        bottom=Side(border_style="thin"),
    )

    right_border = Border(
        left=Side(border_style="dotted"),
        right=Side(border_style="thin"),
        top=Side(border_style="thin"),
        bottom=Side(border_style="thin"),
    )

    if skip_columns:
        skip_columns = skip_columns if isinstance(skip_columns, list) else [skip_columns]
        skip_idxs = [get_column_index(ws, c) - 1 for c in skip_columns]
        compareable_col_idxs = list(set(range(ws.max_column)) - set(skip_idxs))
    else:
        compareable_col_idxs = list(range(ws.max_column))

    for row in ws.iter_rows(
        min_row=2, min_col=0, max_row=ws.max_row, max_col=ws.max_column
    ):
        for col_idx in compareable_col_idxs[::num_ds]:
            equal = []
            for i in range(num_ds - 1):
                equal.append(row[col_idx + i].value == row[col_idx + i + 1].value)

            if not all(equal):
                for i in range(num_ds):
                    row[col_idx + i].fill = yellow_fill

                    if i == 0:
                        row[col_idx].border = left_border
                    elif i == num_ds - 1:
                        row[col_idx + i].border = right_border
                    else:
                        row[col_idx + i].border = middle_border

    wb.save(excel_file)
