import json
import os
import typer
from rich.progress import Progress
import csv
from pathlib import Path

app=typer.Typer()
@app.command("Folder")
def main(Input_folder:Path=typer.Argument(...,help="")):
    pass
@app.command("Single File")
def main():
    pass


if __name__=="__main__":
    app()