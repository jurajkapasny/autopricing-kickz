import pandas as pd
import numpy as np
import pickle
import os.path
import socket

from libs.utils import retry
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# overwrite default timeout
socket.setdefaulttimeout(600)

import logging
logger = logging.getLogger(__name__)

class GoogleSheetsApi:
    
    def __init__(self, path_token, path_client_secret):
        self.service = self.__login_to_gapi(path_token = path_token,
                                            path_client_secret = path_client_secret)
    
    def __login_to_gapi(self, path_token, path_client_secret):
        
        creds = None
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 
                  'https://www.googleapis.com/auth/drive']

        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(path_token):
            with open(path_token, 'rb') as token:
                creds = pickle.load(token)
                
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(path_client_secret, SCOPES)
                creds = flow.run_local_server(port=0)
                
            # Save the credentials for the next run
            with open(path_token, 'wb') as token:
                pickle.dump(creds, token)

        service = build('sheets', 'v4', credentials=creds, cache_discovery=False)

        return service
    
    
    def get_tabs_names(self, sample_spredsheet_id):
        tabs = self.service.spreadsheets().get(spreadsheetId=sample_spredsheet_id)\
                             .execute()\
                             .get('sheets')
        
        return [tab.get("properties").get("title") for tab in tabs]
        
    
    def google_sheet2df(self, sample_spredsheet_id, sample_range_name):
        # Call the Sheets API
        sheet = self.service.spreadsheets()
        result = sheet.values()\
                      .get(spreadsheetId = sample_spredsheet_id,
                           range = sample_range_name)\
                      .execute()
        
        values = result.get('values', [])

        if not values:
            logger.warning(f'Data not found in {sample_range_name}!')
            return None

        header = values[0]   # Assumes first line is header!
        values = values[1:]  # Everything else is data.

        if not values:
            logger.warning(f'Data not found in {sample_range_name}!')
            return None
        
        else:
            all_data = {}
            for col_idx, col_name in enumerate(header):
                column_data = []
                for row in values:
                    try:
                        column_data.append(row[col_idx].strip())
                    except:
                        column_data.append(np.nan)
                all_data[col_name] = column_data

            df = pd.DataFrame(all_data).replace('',np.nan)

            return df
    
    @retry(Exception, total_tries=5, initial_wait=60, backoff_factor=2, logger=logger)    
    def delete_cell_values(self, sample_spredsheet_id, sample_range_name):
        response = ''
        logger.info('Delete status:')
        response = self.service.spreadsheets()\
                               .values()\
                               .clear(
                                    spreadsheetId = sample_spredsheet_id,
                                    range = sample_range_name)\
                                .execute()
        
        self.__print_response(response)
    
    @retry(Exception, total_tries=5, initial_wait=60, backoff_factor=2, logger=logger)    
    def update_cell_values(self, df, sample_spredsheet_id, sample_range_name, with_header=False, valueInputOption='USER_ENTERED'):
        if with_header:
            values = df.T.reset_index().T.values.tolist()
        else:
            values = df.values.tolist()

        response = ''
        logger.info('Update status:')
        response = self.service.spreadsheets()\
                               .values()\
                               .update(
                                 spreadsheetId = sample_spredsheet_id,
                                 valueInputOption = valueInputOption,
                                 range = sample_range_name,
                                 body = dict(
                                    majorDimension = 'ROWS',
                                    values = values)
                                 )\
                               .execute()

        self.__print_response(response)
    
    def __print_response(self,response):
        for r in response:
            logger.info(f'{r}: {response[r]}')