import csv
import typer
from pathlib import Path
from helper import read_json

app = typer.Typer()


@app.command("Single_File")
def main(
    file: Path = typer.Argument(..., help="Input JSON file"),
    field_csv: str = typer.Argument(..., help="Keys to extract, comma-separated"),
    output_file: Path = typer.Option("output1.csv", "--output", "-o", help="Output CSV")
):
    data = read_json(file)

    fields = field_csv.split(",")
    flat = flatten_json(data)

    # Extracted values
    extracted = {field: [] for field in fields}

    # Step 1: Extract normal fields
    for key, value in flat.items():
        for field in fields:
            if field == "duration":
                continue  # skip for now

            if key.endswith(f".{field}") or key == field:
                extracted[field].append(value)

    # Step 2: CALCULATE duration
    if "duration" in fields:

        start_vals = extracted.get("start", [])
        end_vals = extracted.get("end", [])

        duration_list = []
        for s, e in zip(start_vals, end_vals):
            try:
                s = float(s)
                e = float(e)
                duration_list.append(round(e - s, 3))
            except Exception:
                duration_list.append("")

        extracted["duration"] = duration_list
        print("DEBUG → Duration calculated:", duration_list)

    # Step 3: Build CSV rows
    max_rows = max(len(v) for v in extracted.values())

    rows = []
    for i in range(max_rows):
        row = {}
        for field in fields:
            row[field] = extracted[field][i] if i < len(extracted[field]) else ""
        rows.append(row)

    # Step 4: Write CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV created → {output_file}")


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
