import io
import os
import json
import numpy as np
import pandas as pd
import sys
import logging
from glob import glob
from pde_dagster.register.logger import logger
from os import path
from time import time
from threading import Thread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


def drive_create_client():
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    
    work_path = os.path.dirname(os.path.abspath(__file__))
    path_json = glob(os.path.join(os.path.dirname(work_path), "*.json"))
    creds_path = "".join(path_json)
    
    if path.isfile(creds_path):
        credentials = (
            service_account
            .Credentials
            .from_service_account_file(creds_path)
        )

    scoped_credentials = credentials.with_scopes(scopes)

    drive = build('drive', 'v3', credentials=scoped_credentials)

    return drive


def list_files():
    drive = drive_create_client()
    request = drive.files().list().execute()
    files = request.get('files', [])
    files = [
        f for f in files if f['mimeType'] != 'application/vnd.google-apps.folder'
    ]

    if len(files) == 0:
        logger.info('''No se encontraron archivos.
            ¿Tu cuenta de servicio tiene acceso a tu Drive''')
        
    return files


def download_file(file_id):
    try:
        start_time = time()
        drive = drive_create_client()
        request = drive.files().get_media(fileId=file_id)
        f = io.BytesIO()
        downloader = MediaIoBaseDownload(f, request)
    
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f'Archivo cargado en {round(time() - start_time, 2)} segundos')
    
    except HttpError as error:
        print(f'''
        An error occurred while downloading file from Google Drive: {error}'''
        )
        return
    
    return f.getvalue()


def load_file_to_df(f):
    filename, ext = f['name'].split('.')

    assert filename.count('__') == 2, "Archivo le falta tiene muchos '__'s xP"

    location, date, employee = filename.split('__')

    file_content = download_file(f['id'])
    file_df = pd.read_excel(io=file_content)

    file_df['location'] = location
    file_df['date'] = date
    file_df['employee'] = employee

    return file_df


def pack_to_df(files=None, dfs_in=None):
    if dfs_in is not None:
        dfs = dfs_in
    else:
        dfs = []
    
    malformed_filenames = []

    if files is None:
        files = list_files()

    for f in files:
        try:
            file_df = load_file_to_df(f)
            dfs.append(file_df)
        except AssertionError:
            logger.error(f'{f["name"]} no tiene el formato esperado')
            malformed_filenames.append(f["name"])
            continue
    
    if len(dfs) > 0:
        concat_df = pd.concat(dfs)
    else:
        concat_df = pd.DataFrame()
    
    return concat_df, malformed_filenames


def parallel_pack():
    files = list_files()
    print(f'Hay {len(files)} archivos')

    file_chunks = np.array_split(files, 25)

    start_time = time()
    dfs = []
    threads = []

    for chunk in file_chunks:
        t = Thread(target=pack_to_df, args=(chunk, dfs))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print(f'Se cargaron los dataframes en paralelo {round(time() - start_time, 2)} segundos')

    market_df = pd.concat(dfs)
    return market_df