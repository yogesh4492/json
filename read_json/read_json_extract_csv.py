import csv
import typer
from pathlib import Path
# from helper import read_json
import  json

app = typer.Typer()

def read_json(filename, encoding='utf8'):
    """
    read json file from local disk
    filename - name of the json file
    encoding - encoding with which to open the json file, default=utf8
    """
    with open(filename, encoding=encoding) as fp:
        return json.load(fp)

@app.command("Single_File")
def main(
    file: Path = typer.Argument(..., help="Input JSON file"),
    fields: str = typer.Argument(..., help="Keys to extract, comma-separated"),
    output_file: Path = typer.Option("output.csv", "--output", "-o", help="Output CSV")
):
    data = read_json(file)

    fields = fields.split(",")
    flat = flatten_json(data)

    # Collect values separately for each field
    extracted = {field: [] for field in fields}

    for key, value in flat.items():
        for field in fields:
            if key.endswith(f".{field}") or key == field:
                extracted[field].append(value)

    # Prepare rows (maximum rows = max items among fields)
    max_rows = max(len(v) for v in extracted.values())

    rows = []
    for i in range(max_rows):
        row = {}
        for field in fields:
            row[field] = extracted[field][i] if i < len(extracted[field]) else ""
        rows.append(row)

    # Write CSV
    Dump_csv(output_file,fields,rows)
    
    print(f"CSV created with {len(rows)} rows → {output_file}")
def Dump_csv(output_file,fields,data):
    with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            #code for duration if you want in csv 
            
            # for i in data:
            #     ro={}
            #     ro['Duration']=i.get('end')-i.get('start')
            #     ro['Duration']=f"{ro['Duration']:.2f}"
            #     i.update(ro)
                
            writer.writerows(data)

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
