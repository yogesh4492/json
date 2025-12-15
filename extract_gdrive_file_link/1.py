from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaIoBaseUpload

import typer
import json
import csv
import os
import pickle
from pathlib import Path
import glob


app=typer.Typer()
scope=['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']

def auth():
    creds=None
    if os.path.exists("token.pickle"):
        with open('token.pickle',"rb") as r:
            creds=pickle.load(r)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow=InstalledAppFlow.from_client_secrets_file("credentials.json",scope)
            creds=flow.run_local_server(port=0)
            with open("token.pickle","wb") as w:
                pickle.dump(creds,w)
    return creds



def list_files(folder_id,service):
    files=[]
    page_Token=None
    while True:
        resp=service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            spaces="drive",
            fields="nextPageToken,files(id,name,mimeType,size,webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=page_Token

        ).execute()
        for i in resp.get('files',[]):
            if i.get("mimeType")=="application/vnd.google-apps.folder":
                # print(i.get("name"))
                files.extend(list_files(i['id'],service))
            else:
                files.append(i)

        page_Token=resp.get('nextPageToken')
        if not page_Token:
            break
    return files
def dump_csv(filename,data,fields=['FileName','FileLink']):
    with open(filename,"w") as cw:
        csw=csv.DictWriter(cw,fieldnames=fields)
        csw.writeheader()  
        csw.writerows(data)         
@app.command()
def main(folder_id:str=typer.Argument(...,help="input_folder_id"),output_csv:str=typer.Argument(...,help="Output csv name")):
    creds=auth()
    service=build("drive","v3",credentials=creds)
    files=list_files(folder_id,service)
    print(len(files))
    # print(files)
    rows=[]
    for i in files:
        row={}
        row['FileName']=i.get('name')
        row['FileLink']=i.get('webViewLink')
        rows.append(row)

    dump_csv(output_csv,rows)
if __name__=="__main__":
    app()

