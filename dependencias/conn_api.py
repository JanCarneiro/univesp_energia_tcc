import os.path
import json
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/drive']
TOKEN_FILE = '/home/jan/Documentos/tcc/secrets/token.json'

def cria_token():

    creds = None    
    credentials = '/home/jan/Documentos/tcc/secrets/credentials.json'

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials, SCOPES
            )

            creds = flow.run_local_server(port=0, browser='firefox')

        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
    
    return creds

def get_token_from_json():

    with open(TOKEN_FILE, "r") as tokens:
        token_dict = tokens.readline()
        data_dict = json.loads(token_dict)

        token = data_dict['token']
        refresh_token = data_dict['refresh_token']

        return token, refresh_token
    

def insert_file_to_drive(file_path, folder_id, file_name, creds):

    path_files = '/home/jan/Documentos/tcc/secrets/files_id.csv'

    service = build('drive', 'v3', credentials=creds)

    file_metadata = {
        'name': file_name,
        'parents': [folder_id]  
        }
    
    media = MediaFileUpload(file_path, resumable=True)

    if not os.path.exists(path_files):
            with open(path_files, 'w') as ids:
                header = ['id_file', 'nm_base']
                ids.write(header[0] + ',' + header[1])

    df = pd.read_csv(path_files, sep=',')
    qntd_id = df.where(df['nm_base'] == file_name).dropna()

    if int(qntd_id['id_file'].count()) == 0:

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id').execute()
    
        id_file = file.get('id')
   
        with open(path_files, 'a') as ids:
            header = [f'\n{id_file}', f'{file_name}']
            ids.writelines(header[0] + ',' + header[1])

        print(f'Adicionado arquivo {file_name}')

    else:

        id_file = qntd_id['id_file'][int(qntd_id.index[0])]

        file = service.files().update(
            fileId=id_file,            
            media_body=media
            ).execute()

        print(f'Atualizado arquivo {file_name}')

    