import io
import os
import json
import numpy as np
import pandas as pd
import sys
import logging
from glob import glob
from decouple import config
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
    else:
        drive_creds = config("DRIVE_CREDS")
        assert drive_creds is not None, "DRIVE_CREDS environment variable not set"
        credentials = (
            service_account
            .Credentials
            .from_service_account_info(
                json.loads(drive_creds)
            )
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
        print('''No se encontraron archivos.
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

    
    except HttpError as error:
        print(
            f'Ocurrió un error al descargar el archivo'
            f' desde Google Drive: {error}'
        )
        return
    
    return f.getvalue()


def load_file_to_df(f, verbose=False, context=None):
    
    if verbose:
        msg = f"Starting to download {f['name']}"
        if context is not None:
            context.log.info(msg)
        else:
            print(msg)
    
    filename, ext = f['name'].split('.')

    assert filename.count('__') == 2, "Archivo le falta tiene muchos '__'s xP"

    location, date, employee = filename.split('__')

    file_content = download_file(f['id'])
    file_df = pd.read_excel(io=file_content)

    file_df['location'] = location
    file_df['date'] = date
    file_df['employee'] = employee

    return file_df


def pack_to_df(files=None, dfs_in=None, verbose=False, context=None):
    if dfs_in is not None:
        dfs = dfs_in
    else:
        dfs = []
    
    malformed_filenames = []

    if files is None:
        files = list_files()

    for f in files:
        try:
            file_df = load_file_to_df(f, verbose=verbose, context=context)
            dfs.append(file_df)
        except ValueError:
            if verbose:
                msg = f"File name '{f['name']}' is not following the expected format."
                if context is not None:
                    context.log.info(msg)
                else:
                    print(msg)
            malformed_filenames.append(f["name"])
            continue
    
    if len(dfs) > 0:
        concat_df = pd.concat(dfs)
    else:
        concat_df = pd.DataFrame()
    
    return concat_df, malformed_filenames


def parallel_pack():
    files = list_files()
    msg = f"Found {len(files)} files"
    if context is None:
        print(msg)
    else:
        context.log.info(msg)

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

    msg = (
        f"Loading dataframes from Drive in parallel"
        f" took {round(time() - start_time, 2)} seconds"
    )
    if context is None:
        print(msg)
    else:
        context.log.info(msg)

    market_df = pd.concat(dfs)
    return market_df