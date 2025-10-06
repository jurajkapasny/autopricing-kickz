import pyodbc

import pandas as pd
import numpy as np
import datetime as dt
import logging

from libs.google_sheets import GoogleSheetsApi
from libs.help_functions import excel_col_index_to_string
from client_based_code.kickz_code import (
    S3ProductsToScore,
    get_quantities_from_inventory, 
    get_live_styles, 
    get_all_products
)

logger = logging.getLogger(__name__)

class NewProductsAdder:
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
        Nacita products_to_score z S3
        """
        logger.info('Loading products_to_score...')
        return S3ProductsToScore.load_latest().drop_duplicates()
    
    def _load_wait_after_release_settings(self):
        """
        Nacita product_to_score_settings z google sheet
        """
        logger.info('Loading wait_after_relase...')
        wait_after_release_setting = {}

        df_wait_after_release = self.gapi.google_sheet2df(self.settings.gs_spreadsheet_id, 'wait_after_release')
        if df_wait_after_release is not None:
            df_wait_after_release['style'] = df_wait_after_release['style'].str.lower().str.strip()
            df_wait_after_release['wait_after_release'] = df_wait_after_release['wait_after_release'].astype(int, errors='ignore')

            df_wait_after_release = (
                df_wait_after_release
                .drop_duplicates('style', keep='last')
                .set_index('style')
            )

            wait_after_release_setting = df_wait_after_release['wait_after_release'].dropna().to_dict()

        return wait_after_release_setting
    
    def _load_remove_from_new_products(self):
        """
        Nacita znacky, produkty, styly ktore nezahrnut v novych produktoch
        """
        logger.info('Loading remove_from_new_products...')
        # ak su nejake produkty ktore je zakazane pridavat
        try:
            return self.gapi.google_sheet2df(self.settings.gs_spreadsheet_id, 'remove_from_new_products')\
                            .drop_duplicates()\
                            .applymap(lambda s: s.strip().lower() if type(s) == str else s)
        except:
            return None
    
    
    def _load_quantities_in_inventory(self):
        """
        Nacita stav skladu
        struktura: {('brand_1', 'style_1'): 10, ('brand_2',style_2'): 3}
        """
        logger.info('Loading quantities in inventory...')
    
        return get_quantities_from_inventory(as_dict=True)
    
    def _load_category_mapper(self):
        """
        Nacita styly pre jednotlive kategorie
        
        struktura: {'style_1': 'TEAM_SALE', 'style_2': 'IMP'}
        """
        logger.info('Loading category_settings...')
        mapper = {}
        df_category_settings = self.gapi.google_sheet2df(self.settings.gs_spreadsheet_id, 'category_settings')
        
        if df_category_settings is not None:
            for category in df_category_settings.columns.tolist():
                values = [v.strip().lower() for v in df_category_settings[category].dropna().tolist()]
                category_mapper = dict(zip(values, [category]* len(values)))
                mapper.update(category_mapper)
            
        return mapper
    
    def _load_master_switch_styles_settings(self):
        """
        Nacita master_remove tab so stylmi
        
        struktura: {'DE': ["styl_1", "styl_2",...], 'CZ': []}
        """
        df_master_remove = self.gapi.google_sheet2df(self.settings.gs_spreadsheet_id, 'master_remove')
        master_switch_styles_settings = {}
        if df_master_remove is not None:
            df_master_remove.columns = [col.strip() for col in df_master_remove.columns]

            for col in df_master_remove.columns:
                master_switch_styles_settings[col] = df_master_remove[col].dropna().astype(str).str.strip().str.lower().drop_duplicates().tolist()
            
        return master_switch_styles_settings
    
    def _load_master_switch_brands_settings(self):
        """
        Nacita master_remove_brans tab so znackami
        
        struktura: {'DE': ["styl_1", "styl_2",...], 'CZ': []}
        """
        df_master_remove = self.gapi.google_sheet2df(self.settings.gs_spreadsheet_id, 'master_remove_brands')
        master_switch_brands_settings = {}
        if df_master_remove is not None:
            df_master_remove.columns = [col.strip() for col in df_master_remove.columns]
            
            for col in df_master_remove.columns:
                master_switch_brands_settings[col] = df_master_remove[col].dropna().astype(str).str.strip().str.lower().drop_duplicates().tolist()
            
        return master_switch_brands_settings
    
    def _create_country_columns(self, df, countries):
        """
        Vytvori stplce s krajinami
        Priklad: 
            SK__auto_pricing, SK__discount, 
            CZ__auto_pricing, CZ__discount,...
        """
        logger.info(f'Creating country columns ({countries}) ...')
        for c in countries:
            df[f'{c}__auto_pricing'] = 1
            df[f'{c}__discount'] = ''
            
        return df
    
    def _load_all_products(self):
        """
        Nacita vsetky produkty z items
        """
        logger.info('Loading all_products...')
        
        return get_all_products()
        
    def _create_category(self, df, quantities_in_inventory):
        """
        Vytvori kategorie (IMP, ST) pre nove produkty na zaklade skladu 
        """
        logger.info('Creating category column...')
        # Ak je styl na sklade => category=ST inak category=>IMP
        df['category'] = df.apply(lambda row: 'ST' if quantities_in_inventory.get((row['brand'], row['style']), 0) > 0 else 'IMP', axis=1)
        
        return df
    
    def _create_date_added(self, df):
        """
        Vytvori 'date_added' stplec s aktualnym datumom
        """
        logger.info('Creating date_added column...')
        df['date_added'] = pd.to_datetime(dt.date.today())
        return df
    
    def _get_ignored_pruducts_styles(self):
        df_remove_from_new_products = self._load_remove_from_new_products()
        
        if df_remove_from_new_products is None:
            remove_product_name = []
            remove_styles = []
        else:
            # produkty ktore nepridavat
            remove_product_name = (df_remove_from_new_products['brand'].dropna().unique().tolist()
                                 + df_remove_from_new_products['product_name'].dropna().unique().tolist())
            # styly ktore nepridavat
            remove_styles = df_remove_from_new_products['style'].dropna().unique().tolist()

        return remove_product_name,remove_styles
    
    def _update_sheet(self, df, gs_spreadsheet_id, sample_range):
        logger.info(f'Updating data shape {df.shape}...')
        # deleting values from products_to_score
        logger.info(f'Deleting everything from {gs_spreadsheet_id}...')
        self.gapi.delete_cell_values(
            sample_spredsheet_id = gs_spreadsheet_id,
            sample_range_name = sample_range
        )
        
        # inserting new values
        logger.info(f'Inserting new values to {gs_spreadsheet_id}...')
        self.gapi.update_cell_values(
            df = df.replace(np.nan, ''),
            sample_spredsheet_id = gs_spreadsheet_id,
            sample_range_name = sample_range,
            with_header = False
        )
    
    def add_new_products(self, store_in_s3=False):
        """
        Prida nove produkty do products_to_score
        """
        logger.info('New products adder started...')
        
        # nacitanie dat
        df_products_to_score = self._load_products_to_score()
        df_all_products = self._load_all_products()    
        
        # odstranenie produktov ktore uz mame
        df_all_products = df_all_products[~df_all_products['style'].isin(df_products_to_score['style'].unique().tolist())] 
        
        # produkty a styly ktore ignorujeme
        ignore_product_name, ignore_styles = self._get_ignored_pruducts_styles()
        if ignore_product_name: 
            df_all_products = df_all_products[(~df_all_products['product_name'].str.contains('|'.join(ignore_product_name)))]
            
        if ignore_styles:
            df_all_products = df_all_products[(~df_all_products['style'].str.contains('|'.join(ignore_styles)))]
        
        # vytvorenie stplcov
        quantities_in_inventory = self._load_quantities_in_inventory()
        df_all_products = self._create_category(df_all_products, quantities_in_inventory)
        df_all_products = self._create_date_added(df_all_products)
        
        df_final = (
            pd.concat([df_products_to_score, df_all_products])
              .sort_values(by=['category','product_name'])\
              .drop_duplicates(subset=['brand','style'],keep='first')
        )
        
        # vytvorenie slpcov pre skorovane krajiny
        countries = self.settings.countries
        df_final = self._create_country_columns(df_final, countries)
        
        # iba styly ktore sa aktualne vyskytuju na strankach
        styles_on_web = get_live_styles()
        df_final = df_final[df_final['style'].isin(styles_on_web)]
        
        # zmena kategorie
        category_mapper = self._load_category_mapper()
        df_final['category'] = df_final.apply(lambda row: category_mapper.get(row['style'], row['category']) , axis=1)
        
        # wait after relase nastavenie
        wait_after_release_settings = self._load_wait_after_release_settings()
        df_final['wait_after_release'] = df_final['style'].apply(lambda style: wait_after_release_settings.get(style, 21))
        
        # master switch nastavenie
        master_switch_styles_settings = self._load_master_switch_styles_settings()
        master_switch_brands_settings = self._load_master_switch_brands_settings()
        df_final['master_switch'] = df_final.apply(
            lambda row: 
            0 if ((row['style'] in master_switch_styles_settings.get('style_for_removal',[]) or (row['brand'] in master_switch_brands_settings.get('brand_for_removal',[])))) 
            else 1,
            axis = 1
        )
        
        for country_code in countries:
            df_final[f'{country_code}__auto_pricing'] = df_final.apply(
                lambda row: 
                0 if ((row['style'] in master_switch_styles_settings.get(country_code,[]) or (row['brand'] in master_switch_brands_settings.get(country_code,[])))) 
                else 1,
                axis = 1
            )
        
        # changed last days nastavenie
        df_final['changed_last_days'] = 0
        
        # sort values
        df_final = df_final.sort_values(by=['category','product_name'])
        
        # store in S3
        if store_in_s3:
            S3ProductsToScore.store(df_final)
        
        # docasne
        df_gs = df_final.copy(deep=True)
        df_gs['date_added'] = df_gs['date_added'].astype(str)
        self._update_sheet(
            df_gs, 
            self.settings.gs_spreadsheet_id, 
            'products_to_score!A2:ZZZ1000000'
        )
        # docasne end
        
        return df_final
        
        logger.info('Finished succesfully...')