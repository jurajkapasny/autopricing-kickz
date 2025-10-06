import logging
import numpy as np
import datetime as dt

from libs.google_sheets import GoogleSheetsApi
from client_based_code.kickz_code import (
    S3ProductsToScore,
    get_orders
)

logger = logging.getLogger(__name__)

class UpdateProductsToScrape:
    def __init__(self, settings):
        self.settings = settings
        self.gapi = self._initialize_gapi()
    
    def _initialize_gapi(self):
        """
        Inicializuje Google sheets API
        """
        return GoogleSheetsApi(
            path_token = self.settings.gs_path_token,
            path_client_secret = self.settings.gs_path_client_secret
        )
    
    def _load_products_to_score(self):
        """
        Nacita products_to_score tab z GS
        """
        logger.info('Loading products_to_score...')
        df_products_to_score = S3ProductsToScore.load_latest()[['brand','product_name','style']]
        return df_products_to_score
    
    def _load_orders(self, history_days=30):
        """
        Nacita orders
        """
        logger.info('Loading orders...')
        to_date = dt.date.today()
        from_date = to_date - dt.timedelta(days=history_days)
        
        return get_orders(from_date=from_date, to_date=to_date)
    
    def _update_sheet(self, df, sample_spreadsheet_id, sample_range):
        logger.info(f'Updating data shape {df.shape}...')
        # deleting values from products_to_score
        logger.info(f'Deleting everything from {sample_spreadsheet_id}...')
        self.gapi.delete_cell_values(
            sample_spredsheet_id = sample_spreadsheet_id,
            sample_range_name = sample_range
        )
        
        # inserting new values
        logger.info(f'Inserting new values to {sample_spreadsheet_id}...')
        self.gapi.update_cell_values(
            df = df.replace(np.nan, ''),
            sample_spredsheet_id = sample_spreadsheet_id,
            sample_range_name = sample_range,
            with_header = False
        )
    
    def run(self, orders_history_days=30, min_unit_price=20, search_terms_limit=3000):
        logger.info('Updater started...')
        df_products_to_score = self._load_products_to_score()
        df_orders = self._load_orders(orders_history_days)
        
        logger.info('Filtering orders...')
        df_orders = df_orders[df_orders['unit_price_vat_excl'] >= min_unit_price]
        df_orders_grouped = df_orders.groupby(['brand','style'], as_index=False)['quantity'].sum()
        
        logger.info('Selecting top products to scrape...')
        df_final = (
            df_products_to_score[['brand','style','product_name']]
            .merge(df_orders_grouped, on=['brand','style'], how='inner')
            .fillna(0)
            .sort_values('quantity',ascending=False)\
            .reset_index(drop=True)
        )
        df_final = df_final.head(search_terms_limit)
        
        self._update_sheet(
            df = df_final,
            sample_spreadsheet_id = self.settings.gs_spreadsheet_id,
            sample_range = 'products_to_watch!A2:ZZZ1000000'
        )
        
        logger.info('Updater finished succcesfully!!')