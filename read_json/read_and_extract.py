import csv
import typer
from helper import dump_csv, read_json
from rich.progress import Progress
from pathlib import Path

app = typer.Typer()


@app.command("Single_File")
def main(
    file: Path = typer.Argument(..., help="Input JSON file"),
    field_csv: str = typer.Argument(..., help="Keys to extract, comma separated"),
    output_file: Path = typer.Option("outp.csv", "--output", "-o", help="Output CSV file"),
):
    data = read_json(file)

    fields = field_csv.split(",") 
    print(fields) # user-required columns
    flat_data = flatten_json(data)  # flatten JSON into key:value pairs

    # Create a single CSV row
    row = {field: flat_data.get(field, "") for field in fields}

    # Write CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerow(row)

    print("CSV created:", output_file)


def flatten_json(data, parent_key="", out=None):
    """Flatten nested JSON objects into key-value pairs"""
    if out is None:
        out = {}

    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            flatten_json(value, full_key, out)

    elif isinstance(data, list):
        for i, item in enumerate(data):
            full_key = f"{parent_key}[{i}]"
            flatten_json(item, full_key, out)

    else:
        out[parent_key] = data

    return out


if __name__ == "__main__":
    app()
