import json
import typer
import os
from pathlib import Path
from rich.progress import Progress

app=typer.Typer()

remove_keys=['type','conventionInfo',"domains","primaryVariety","annotatorInfo","taskStatus","segmentation","speakerId"]

def read_json(file):
    try:
        if os.path.exists(file):
            with open(file,"r",encoding="utf-8") as r:
                    return json.load(r)    
    except Exception as e:
        print(e)
              
def dump_json(file,data):
     with open(file,"w",encoding="utf-8") as w:
          json.dump(data,w,ensure_ascii=False,indent=2)

def clean_data(data):
    if isinstance(data,dict):
            for i in remove_keys:
                if i in data:
                    data.pop(i)
                elif isinstance(data['value'],dict):
                        for i in remove_keys:
                            if i in data['value']:
                                data['value'].pop(i)
                else:
                        print(i,"keys are not availbale ")
    return data


@app.command("Multiple_Files_or_folder")
def main(input_folder:Path=typer.Argument(...,help="Input Folder That Contain Original Json File")
         ,output_folder:Path=typer.Argument(...,help="Output_folder To store The clean Json File")):
     files=os.listdir(input_folder)
     os.makedirs(output_folder,exist_ok=True)
     with Progress() as p:
          task=p.add_task("Processing...",total=len(files))
          for i in files:
               input_file=os.path.join(input_folder,i)
            #    print(input_file)
               output_file=os.path.join(output_folder,i)
               data=read_json(input_file)
               update_data=clean_data(data)
               dump_json(output_file,update_data)
               p.update(task,advance=1) 
            
     
    

@app.command("Single_file")
def main(file:Path=typer.Argument(...,help="Pass Json File")):
    data=read_json(file)
    upadted_data=clean_data(data)
    dump_json(f"{file.stem}_updated.json",upadted_data)
    
    

if __name__=="__main__":
    app()

