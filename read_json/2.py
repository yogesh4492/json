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

    fields = field_csv.split(",")  # user fields
    flat = flatten_json(data)      # full flatten

    # Find values for each requested key (no full path needed)
    row = {}
    for field in fields:
        matches = [value for key, value in flat.items() if key.endswith(f".{field}") or key == field]
        
        # if multiple values found → join them
        if matches:
            row[field] = "; ".join(map(str, matches))
        else:
            row[field] = ""

    # Write to CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerow(row)

    print(f"CSV created → {output_file}")


def flatten_json(data, parent_key="", out=None):
    """Flatten JSON with full path keys"""
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
