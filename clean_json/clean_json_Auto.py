import json
import typer
import os
from pathlib import Path
from rich.progress import Progress

app = typer.Typer()

# Keys to remove automatically from full JSON structure (any level)

def read_json(file):
    try:
        with open(file, "r", encoding="utf-8") as r:
            return json.load(r)
    except Exception as e:
        print(f"Error reading {file}: {e}")
        return None


def dump_json(file, data):
    with open(file, "w", encoding="utf-8") as w:
        json.dump(data, w, ensure_ascii=False, indent=2)


# ---------------------------------------------------------
# ⭐ AUTO-DETECT + CLEAN FUNCTION (recursive)
# ---------------------------------------------------------
def clean_data(data,REMOVE_KEYS):
    if isinstance(data, dict):
        # remove keys in current dict
        return {
            key: clean_data(value,REMOVE_KEYS)
            for key, value in data.items()
            if key not in REMOVE_KEYS
        }

    elif isinstance(data, list):
        # clean each list item
        return [clean_data(item,REMOVE_KEYS) for item in data]

    else:
        return data
# ---------------------------------------------------------


@app.command("Multiple_Files_or_folder")
def clean_folder(
    input_folder: Path = typer.Argument(..., help="Folder containing JSON files"),
    output_folder: Path = typer.Argument(..., help="Folder to store cleaned JSONs"),
    keys:str=typer.Argument(...,help="Input_keys You want To Remove...")
): 
    keys=keys.split(",")
    os.makedirs(output_folder, exist_ok=True)

    files = [f for f in os.listdir(input_folder) if f.endswith(".json")]

    with Progress() as p:
        task = p.add_task("Processing...", total=len(files))

        for file in files:
            input_file = input_folder / file
            output_file = output_folder / file

            data = read_json(input_file)
            if data is None:
                continue

            cleaned = clean_data(data,keys)
            dump_json(output_file, cleaned)

            p.update(task, advance=1)


@app.command("Single_File")
def clean_single(
    file: Path = typer.Argument(..., help="Input JSON file")
    ,keys:str=typer.Argument(...,help="input_keys which You want To remove...")
):
    keys=keys.split(",")
    data = read_json(file)
    cleaned = clean_data(data,keys)

    output_file = file.with_name(f"{file.stem}_clean.json")
    dump_json(output_file, cleaned)

    print(f"Cleaned JSON saved → {output_file}")


if __name__ == "__main__":
    app()
    "type", "conventionInfo", "domains", "primaryVariety",
    "annotatorInfo", "taskStatus", "segmentation", "speakerId"