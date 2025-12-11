# import json
# from rich.progress import Progress
# import typer
# import os
# from pathlib import Path
# from rich.progress import Progress

# app=typer.Typer()

# remove_keys=['type','conventionInfo',"domains","primaryVariety","annotatorInfo","taskStatus","segmentation","speakerId"]

# def read_json(file):
#     try:
#         if os.path.exists(file):
#             with open(file,"r",encoding="utf-8") as r:
#                     return json.load(r)    
#     except Exception as e:
#         print(e)
              
# def dump_json(file,data):
#      with open(file,"w",encoding="utf-8") as w:
#           json.dump(data,w,ensure_ascii=False,indent=2)

# def clean_data(data):
#     if isinstance(data,dict):
#             for i in remove_keys:
#                 if i in data:
#                     data.pop(i)
#                 elif isinstance(data['value'],dict):
#                         for i in remove_keys:
#                             if i in data['value']:
#                                 data['value'].pop(i)
#                 else:
#                         print(i,"keys are not availbale ")
#     return data


# @app.command("Multiple_Files_or_folder")
# def main(input_folder:Path=typer.Argument(...,help="Input Folder That Contain Original Json File")
#          ,output_folder:Path=typer.Argument(...,help="Output_folder To store The clean Json File")):
#      files=os.listdir(input_folder)
#      os.makedirs(output_folder,exist_ok=True)
#      with Progress() as p:
#           task=p.add_task("Processing...",total=len(files))
#           for i in files:
#                input_file=os.path.join(input_folder,i)
#             #    print(input_file)
#                output_file=os.path.join(output_folder,i)
#                data=read_json(input_file)
#                update_data=clean_data(data)
#                dump_json(output_file,update_data)
#                p.update(task,advance=1) 
            
     
    

# @app.command("Single_file")
# def main(file:Path=typer.Argument(...,help="Pass Json File")
#          ,output:str=typer.Option("extract_json.json","--output","-o",help="Enter the output File Name ")):
#     data=read_json(file)
#     upadted_data=clean_data(data)
#     dump_json(f"{file.stem}_updated.json",upadted_data)
    
    

# if __name__=="__main__":
#     app()

"""Above Code Is For The Clean Json Files"""


# -----------------------------------------------------------------------------------------------------------------

import csv
import typer
from helper import dump_csv,read_json
from rich.progress import Progress
from pathlib import Path

app=typer.Typer()

@app.command("Single_File")
def main(file:Path=typer.Argument(...,help="Input Json File")
         ,field_csv:str=typer.Argument(...,help="Enter Key You Want in string ")
         ,output_file:Path=typer.Option("output.csv","--output","-o",help="Output Csv file name")):
    data=read_json(file)
    fields=field_csv.split(",")
    keys=extract_all_keys(data)
    key=keys_to_dict(keys)
    
    # print(key)
    # k=[]
    # for i in key.get(key):
    #     pass 

    # dump_csv(output_file,data)
    write_csv_with_fields(fields,key,output_file)


    
def extract_all_keys(data, parent_key="", keys=None):
    if keys is None:
        keys = set()

    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            keys.add(full_key)
            extract_all_keys(value, full_key, keys)

    elif isinstance(data, list):
        for item in data:
            extract_all_keys(item, parent_key, keys)

    return keys
# 
def keys_to_dict(keys):
    return {key:"" for key in sorted(keys)}



def write_csv_with_fields(field,field_dict, csv_file="schema.csv"):
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=field_dict)
        writer.writeheader()
        
        # row={for i in jsocode json_   n}
        # rows={}
#         for i in 


@app.command("Folder")
def main():
    pass

if __name__=="__main__":
    app()



