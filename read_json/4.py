import csv
import typer
from pathlib import Path
from helper import read_json

app = typer.Typer()


@app.command("Single_File")
def main(
    file: Path = typer.Argument(..., help="Input JSON file"),
    field_csv: str = typer.Argument(..., help="Keys to extract, comma-separated"),
    output_file: Path = typer.Option("output.csv", "--output", "-o", help="Output CSV")
):
    data = read_json(file)

    fields = field_csv.split(",")
    flat = flatten_json(data)

    # Extracted values for fields
    extracted = {field: [] for field in fields}

    # Pre-fill real JSON fields
    for key, value in flat.items():
        for field in fields:
            if field in ("duration",):  
                continue  # skip calculated fields for now
            if key.endswith(f".{field}") or key == field:
                extracted[field].append(value)

    # ====== CALCULATED FIELD: duration ======
    if "duration" in fields:
        start_vals = extracted.get("start", [])
        end_vals = extracted.get("end", [])

        duration_list = []
        for s, e in zip(start_vals, end_vals):
            try:
                duration_list.append(float(e) - float(s))
            except:
                duration_list.append("")

        extracted["duration"] = duration_list
    # ========================================

    # Prepare rows
    max_rows = max(len(v) for v in extracted.values())

    rows = []
    for i in range(max_rows):
        row = {}
        for field in fields:
            row[field] = extracted[field][i] if i < len(extracted[field]) else ""
        rows.append(row)

    # Write CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV created with {len(rows)} rows → {output_file}")


def flatten_json(data, parent_key="", out=None):
    if out is None:
        out = {}

    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            flatten_json(value, new_key, out)

    elif isinstance(data, list):
        for index, item in enumerate(data):
            new_key = f"{parent_key}[{index}]"
            flatten_json(item, new_key, out)

    else:
        out[parent_key] = data

    return out


if __name__ == "__main__":
    app()
