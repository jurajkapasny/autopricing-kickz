import json
import pandas as pd
import numpy as np
import datetime as dt
from libs.help_functions import (
    get_conversion_rates,
    countryCompetitors2dict,
    clean_country_competitors,
    stylesDiscounts2dict,
    stylesAutoPricing2dict,
    changedLastDays2dict,
    productsStyles2dict,
    discountLevels2dict,
    stylesCategory2dict,
    waitAfterRelease2dict,
    is_important_competitor,
    is_our_shop,
    get_country_code_from_url,
    minMaxDisctount2dict,
    COUNTRY_CODE_CURRENCY_MAPPER,
    df_to_nested_dict,
    upload_dataframe_to_azure_blob_storage
)
from libs.google_sheets import GoogleSheetsApi
from client_based_code.kickz_code import *

# debug
import functools
import psutil
import logging

# logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

debug_durations = []
def timeit(func):
    @functools.wraps(func)
    def wrapper_timeit(*args, **kwargs):
        start_time = dt.datetime.now()
        memory_start = psutil.virtual_memory().used / 1000000000
        value = func(*args, **kwargs)
        end_time = dt.datetime.now()
        memory_end = psutil.virtual_memory().used / 1000000000
        
        debug_durations.append(
            {
                'method': func.__name__,
                'start_time': start_time,
                'end_time': end_time,
                'duration': end_time - start_time,
                'memory_start': memory_start,
                'memory_end': memory_end
            }
        )

        return value
    return wrapper_timeit

class PricingLogic:
    
    def __init__(self, settings, category = None):
        self.settings = settings
        self.category = self._parse_category_type(category)
        self.methods_durations = debug_durations
    
    @timeit
    def _parse_category_type(self,category):
        if isinstance(category,list) or category is None:
            return category
        else:
            raise Exception("Category must be list or None!")
    
    @timeit
    def _get_current_time(self):
        return dt.datetime.now()
        
    @timeit   
    def _load_conversion_rates(self):
        """
        Nacita aktualne konverzne kurzy z ECB pre EURO
        struktura: {'EUR': 1, 'USD': 1.12, 'JPY': 120.31, ... }
        """
        
        self.conversion_rates = get_conversion_rates()
        
    @timeit    
    def _load_data_from_google_sheets(self, sample_spreadsheet_id, path_token, path_client_secret):
        """
        Nacitanie dat z Google Sheetu
        """
        
        gapi = GoogleSheetsApi(path_token = path_token,
                               path_client_secret = path_client_secret)
        
        self._load_competitors(gapi, sample_spreadsheet_id)
        self._load_discount_levels(gapi, sample_spreadsheet_id)
        self._load_discount_levels_override(gapi, sample_spreadsheet_id)
        self._load_brand_discounts(gapi, sample_spreadsheet_id)
        self._load_team_sale_discounts(gapi, sample_spreadsheet_id)
        self._load_dropshipment_discounts(gapi, sample_spreadsheet_id)
        self._load_carryovers_discounts(gapi, sample_spreadsheet_id)
        self._load_teamsport_overstock_discounts(gapi, sample_spreadsheet_id)
        self._load_total_clearance_discounts(gapi, sample_spreadsheet_id)
        self._load_indoor_shoes_discounts(gapi, sample_spreadsheet_id)
        self._load_st_settings(gapi, sample_spreadsheet_id)
        self._load_margin_settings(gapi, sample_spreadsheet_id)
        self._load_destroy_competitors_discounts(gapi, sample_spreadsheet_id)
        self._load_complementary_styles(gapi, sample_spreadsheet_id)
        self._load_pricing_groups_settings(gapi, sample_spreadsheet_id)
        self._load_ST_style_season_length_override(gapi, sample_spreadsheet_id)
        
    @timeit
    def _load_products_to_score(self):
        """
        Nacita products_to_score z S3
        """
        df_products_to_score = S3ProductsToScore.load_latest()
        self._load_product_brand_mapper(df_products_to_score)
        self._load_discount_override(df_products_to_score)
        self._load_score_style_in_country(df_products_to_score)
        self._load_changed_last_days_settings(df_products_to_score)
        self._load_products_styles(df_products_to_score)
        self._load_wait_after_release(df_products_to_score)
          
    @timeit
    def _load_competitors(self, gapi, sample_spreadsheet_id):
        """
        Nacita konkurenciu z google sheetu
        struktura: {'SK': ['nike', 'adidas', ...], 'AT': ['zalando','footlocker',...]}
        """
        
        df_competitors = gapi.google_sheet2df(sample_spreadsheet_id, 'relevant_competitors')
        
        # dictionary
        country_competitors = countryCompetitors2dict(df_competitors)
        
        # Ocisti konkurenciu
        # 'store.nike.com' => 'nike'
        # 'https://www.runningpro.sk/' => 'runningpro'
        # ...
        self.country_competitors = clean_country_competitors(country_competitors)
    
    @timeit
    def _load_product_brand_mapper(self, df_products_to_score):
        """
        Naacita mapper z produktu na znacku
        struktura: {'product_1': 'brand_1', 'product_2': 'brand_2', ...}
        """
        
        # dict
        self.product_brand_mapper = df_products_to_score[['product_name','brand']].drop_duplicates()\
                                                                              .set_index('product_name')\
                                                                              .to_dict()\
                                                                              .get('brand')
    @timeit
    def _load_discount_override(self, df_products_to_score):
        """
        Nacita maximalnu moznu zlavu pre kazdy styl
        struktura: {'styl_1': {'DE': 0.15, 'SK': 0.2,...}, 'styl_2': {'DE': 0.3, ...}, ...}
        """
    
        # dict
        self.discount_override = stylesDiscounts2dict(df_products_to_score)
    
    @timeit
    def _load_score_style_in_country(self, df_products_to_score):
        """
        Nacita boolean ci skorovat v danej krajine
        struktura: {'styl_1': {'DE': True, 'SK': False,...}, 'styl_2': {'DE': False, ...}, ...}
        """
        
        # dict
        self.score_style_in_country = stylesAutoPricing2dict(df_products_to_score)
    
    @timeit
    def _load_changed_last_days_settings(self, df_products_to_score):
        """
        Nacita po kolkych dnoch sa moze zmenit cena 
        struktura: {'style_1': 2, 'style_2': 14}
        """
        
        df_products_to_score['changed_last_days'] = df_products_to_score['changed_last_days'].fillna(0).astype(int)

        # dict
        self.changed_last_days_settings = df_products_to_score.drop_duplicates(subset=['style'], keep = 'last')[['style','changed_last_days']]\
                                                     .set_index('style')\
                                                     .to_dict('dict')\
                                                     .get('changed_last_days')
    
    @timeit
    def _load_products_styles(self, df_products_to_score):
        """
        Nacita vsetky styly z Google Sheetu
        styles:
            struktura : ['styl_1', 'styl_2', ....]
        
        products:
            struktura: ['prod_1', 'prod_2', .... ]
        """
        
        # get products to watch
        df_products_to_score = df_products_to_score.drop_duplicates(subset=['style'], keep = 'last')
        df_products_to_score['date_added'] = pd.to_datetime(df_products_to_score['date_added'])
        df_products_to_score.to_csv('products_to_score_last_run.csv',index=False)
        
        if self.category is not None:
            df_products_to_score = df_products_to_score[df_products_to_score['category'].isin(self.category)]
            
        self.prods_styles = productsStyles2dict(df_products_to_score)
        self.styles_prods_mapper = df_products_to_score[['product_name','style']].set_index('style').to_dict().get('product_name')
        self.master_switch = df_products_to_score[['style','master_switch']].set_index('style').astype(int).astype(bool).to_dict().get('master_switch')
        self.products = df_products_to_score['product_name'].str.lower().unique().tolist()
        self.styles = df_products_to_score['style'].str.lower().unique().tolist()
        self.country_codes = self.settings.countries
        self.style_category = stylesCategory2dict(df_products_to_score)
        self.date_added_mapper = df_products_to_score[['date_added','style']].set_index('style').to_dict().get('date_added')
        
    @timeit
    def _load_google_ads(self):
        """
        Nacita google ads
        """
        def adjust_gads_data(df_gads):
            df_gads.loc[df_gads['clicks'] > df_gads['impressions'],'impressions'] = df_gads.loc[df_gads['clicks']> df_gads['impressions'],'clicks']
            df_gads['ctr'] = df_gads['clicks'].divide(df_gads['impressions']).fillna(0)
            
            return df_gads
        
        today = self.run_time.date()
        yesterday = today - dt.timedelta(days=1)
        df_gads_yesterday = adjust_gads_data(get_google_ads_data(yesterday, yesterday))

                
        last_week_start = today - dt.timedelta(7)
        week_before_start = today - dt.timedelta(14)
        
        df_gads_last_week = adjust_gads_data(get_google_ads_data(last_week_start))
        df_gads_week_before = adjust_gads_data(get_google_ads_data(week_before_start, last_week_start))
        
        df_gads_ratio = df_gads_last_week.merge(
            df_gads_week_before,
            left_on = ['country_code','style','brand'],
            right_on = ['country_code','style', 'brand'],
            suffixes = ('_last_week','_week_before')
            )\
             .assign(
                clicks_ratio = lambda x: x['clicks_last_week'] / x['clicks_week_before'],
                impresions_ratio = lambda x: x['impressions_last_week'] / x['impressions_week_before'],
                ctr_ratio =lambda x: x['ctr_last_week'] / x['ctr_week_before']
            )\
            .fillna(0)

        df_gads_ratio = df_gads_ratio[['country_code','style','clicks_ratio', 'impresions_ratio', 'ctr_ratio']]
        
        """
        struktura:
            {'country_code': {'style': {'clicks': 0, 'ctr': 0.0, 'impressions': 1}}}
        """
        self.gapi_yesterday_products_ads_dict = df_to_nested_dict(df_gads_yesterday, 'country_code', 'style', ['clicks', 'ctr','impressions'])
                                             
        """
        struktura:
            {'country_code': {'style': {'clicks_ratio': 0.0, 'impresions_ratio': 0.0, 'ctr_ratio': 0.0}}}
        """
        self.df_gapi_714_ratios = df_gads_ratio
        
    @timeit
    def _load_discount_levels(self, gapi, sample_spreadsheet_id):
        """
        Nacita discount levels pre kazdu krajinu pripadne pre HARD_SALE, SOFT_SALE, ENTRY_SALE
        
        struktura:
            {'SOFT_SALE': {('adidas', 'RO'): {'Season length (weeks)': 8.0,
                                                   'Discount Level 1': 0.25,
                                                   'Discount Level 2': 0.35,
                                                   'Discount Level 3': 0.4,
                                                   'Discount Level 4': 0.45,
                                                   'Discount Level 5': 0.5},
        """
        tabs = gapi.get_tabs_names(sample_spreadsheet_id)
        discount_levels_tabs = [tab for tab in tabs if '__discount_levels' in tab]

        discount_levels = {}
        for tab_name in discount_levels_tabs:
            main_index = tab_name.split('__')[0]
            df_discount_levels = gapi.google_sheet2df(sample_spreadsheet_id, tab_name)
            discount_levels[main_index] = discountLevels2dict(df_discount_levels)

        self.discount_levels = discount_levels
        
    @timeit
    def _load_brand_discounts(self,gapi, sample_spreadsheet_id):
        """
        Nacita zlavy pre znacku
        
        struktura:
        {('adidas', 'SK'): 0.25,
         ('adidas', 'CZ'): 0.45,
         ('nike',   'DE'): 0.15,
         ('nike',   'ES'): 0.15,
         ('nike',   'FR'): 0.25,
         ...
         }
        """        
        df_brand_discount = gapi.google_sheet2df(sample_spreadsheet_id, 'brand_discounts_imp')
        
        self.brand_discount = df_brand_discount.assign(brand=lambda x: x.brand.str.lower().str.strip())\
                                               .dropna(subset=['brand'])\
                                               .drop_duplicates('brand', keep='last')\
                                               .set_index('brand')\
                                               .fillna(0)\
                                               .astype(float)\
                                               .divide(100)\
                                               .rename(columns={col: col.split('__')[0] for col in df_brand_discount.columns})\
                                               .stack()\
                                               .to_frame('discount')\
                                               .to_dict()\
                                               .get('discount')
    
    @timeit
    def _load_team_sale_discounts(self, gapi, sample_spreadsheet_id):
        """
        Nacita zlavy pre team sales vypredaj (category = TEAM_SALE)
        
        struktura:
        {'puma': {'min_discount': 0.3, 'max_discount': 0.8}}
        """
        df = gapi.google_sheet2df(sample_spreadsheet_id, 'TEAM_SALE_discounts')
        
        if df is not None:
            self.team_sale_discounts = minMaxDisctount2dict(df)
        else:
            self.team_sale_discounts = {}
            
    @timeit
    def _load_dropshipment_discounts(self, gapi, sample_spreadsheet_id):
        """
        Nacita zlavy pre team sales vypredaj (category = DROPSHIPMENT)
        
        struktura:
        {'puma': {'min_discount': 0.3, 'max_discount': 0.8}}
        """
        df = gapi.google_sheet2df(sample_spreadsheet_id, 'DROPSHIPMENT_discounts')
        
        if df is not None:
            self.dropshipment_discounts = minMaxDisctount2dict(df)
        else:
            self.dropshipment_discounts = {}
    
    @timeit
    def _load_carryovers_discounts(self, gapi, sample_spreadsheet_id):
        """
        Nacita zlavy pre carryovers (category = CARRYOVERS)
        
        struktura:
        {'puma': {'min_discount': 0.3, 'max_discount': 0.8}}
        """
        df = gapi.google_sheet2df(sample_spreadsheet_id, 'CARRYOVERS_discounts')
        if df is not None:
            self.carryovers_discounts = minMaxDisctount2dict(df)
        else:
            self.carryovers_discounts = {}
            
    @timeit
    def _load_teamsport_overstock_discounts(self, gapi, sample_spreadsheet_id):
        """
        Nacita zlavy pre (category = TEAMSPORT_OVERSTOCK)
        
        struktura:
        {'puma': {'min_discount': 0.3, 'max_discount': 0.8}}
        """
        df = gapi.google_sheet2df(sample_spreadsheet_id, 'TEAMSPORT_OVERSTOCK_discounts')
        if df is not None:
            self.teamsport_overstock_discounts = minMaxDisctount2dict(df)
        else:
            self.teamsport_overstock_discounts = {}
            
    @timeit
    def _load_total_clearance_discounts(self, gapi, sample_spreadsheet_id):
        """
        Nacita zlavy pre (category = TOTAL_CLEARANCE)
        
        struktura:
        {'puma': {'min_discount': 0.3, 'max_discount': 0.8}}
        """
        df = gapi.google_sheet2df(sample_spreadsheet_id, 'TOTAL_CLEARANCE_discounts')
        if df is not None:
            self.total_clearance_discounts = minMaxDisctount2dict(df)
        else:
            self.total_clearance_discounts = {}
    
    @timeit
    def _load_indoor_shoes_discounts(self, gapi, sample_spreadsheet_id):
        """
        Nacita zlavy pre (category = INDOOR_SHOES)
        
        struktura:
        {'puma': {'min_discount': 0.3, 'max_discount': 0.8}}
        """
        df = gapi.google_sheet2df(sample_spreadsheet_id, 'INDOOR_SHOES_discounts')
        if df is not None:
            self.indoor_shoes_discounts = minMaxDisctount2dict(df)
        else:
            self.indoor_shoes_discounts = {}
    
    @timeit
    def _load_st_settings(self, gapi, sample_spreadsheet_id):
        """
        Nacita nastavenie ST produktov
        
        struktura:
        {('SK', 'football'): {'setting': 'GENERAL', 'rate_pct': 8.0},
         ('CZ', 'football')': {'setting': 'GENERAL', 'rate_pct': 8.0},...}"""

        df = gapi.google_sheet2df(sample_spreadsheet_id, 'ST_settings')
        if df is not None:
            df['rate_pct'] = df['rate_pct'].astype(float)
            df['category'] = df['category'].str.strip()

            self.st_settings = df.drop_duplicates(subset=['country_code','category'], keep='last')\
                                             .set_index(['country_code','category'])\
                                             .to_dict('index')
        else:
            self.st_settings = {}
        
    @timeit
    def _load_ST_style_season_length_override(self, gapi, sample_spreadsheet_id):
        """
        Nacita prepisanu dlzku sezony pre jednotlive styly
        struktura:
        {
            'gw4241': {'SK': 10.0, 'CZ': 10.0, 'DE': 10.0, ...},
            'abc': {'Sk': 15, ...},
        }
        """
        df_style_season_length_override = gapi.google_sheet2df(sample_spreadsheet_id, 'ST_season_length_override')
        if df_style_season_length_override is not None:
            style_season_length_override = df_style_season_length_override.drop('note', axis=1)\
                                                                           .drop_duplicates('style')\
                                                                           .rename(columns = {col: col.split('__')[0] for col in df_style_season_length_override.columns})\
                                                                           .assign(style = lambda df: df_style_season_length_override['style'].str.lower().str.strip())\
                                                                           .dropna()\
                                                                           .set_index('style')\
                                                                           .astype(float)\
                                                                           .to_dict(orient='index')
        else:
            style_season_length_override = {}
        
        self.style_season_length_override = style_season_length_override
    
    @timeit
    def _load_margin_settings(self, gapi, sample_spreadsheet_id):
        """
        Nacita nastavenie margin 
        
        struktura:
        {'SK': {'target_margin': 35,'use_in_country': False}, 'CZ': {'target_margin': 38, 'use_in_country': True}, ...}
        """
        df_margin_settings = gapi.google_sheet2df(sample_spreadsheet_id, 'margin_settings')
        df_margin_settings['target_margin'] = df_margin_settings['target_margin'].astype(float)
        df_margin_settings['use_in_country'] = df_margin_settings['use_in_country'].astype(int).fillna(0).astype(bool)
        
        self.margin_settings = df_margin_settings.drop_duplicates(subset='country_code', keep='last').set_index('country_code').to_dict('index')
    
    @timeit
    def _load_destroy_competitors_discounts(self, gapi, sample_spreadsheet_id):
        """
        Nacita maximalne mozne zlavy pre discount_competitors
        
        struktura:
        {(style1, country_code1): 0.3, (style2, country_code2): 0.1, ...}
        """
        df_destroy_competitors = gapi.google_sheet2df(sample_spreadsheet_id, 'destroy_competitors')
        if df_destroy_competitors is None:
            destroy_competitors_discount = {}
        
        else:
            df_destroy_competitors['max_discount'] = df_destroy_competitors['max_discount'].astype(float).divide(100)  #konverzia z percent na desatine cisla
            destroy_competitors_discount = df_destroy_competitors.dropna()\
                                                                  .drop_duplicates(['style','country_code'])\
                                                                  .set_index(['style','country_code'])\
                                                                  .to_dict()\
                                                                  .get('max_discount')
            
        self.destroy_competitors_discount = destroy_competitors_discount
    
    @timeit
    def _load_wait_after_release(self, df_products_to_score):
        """
        Nacita kolko dni po uvedeni styli na trh sa styl neskoruje
        
        struktura:
        {'style1': 12 , 'style2': 14, ....}
        """
        # dict
        self.wait_after_release = waitAfterRelease2dict(df_products_to_score)
    
    @timeit
    def _load_complementary_styles(self, gapi,sample_spreadsheet_id):
        """
        Nacita ktorym smerom je mozne hybat ceny pre komplenetarne styly
        
        struktura:
        ['INCREASE','DECREASE']
        """
        
        df_complementary_styles = gapi.google_sheet2df(sample_spreadsheet_id, 'complementary_styles')
        
        self.complementary_styles_allowed_change = df_complementary_styles[df_complementary_styles['setting'] =='1']['allow'].unique().tolist()
       
    @timeit
    def _load_pricing_groups_settings(self, gapi, sample_spreadsheet_id):
        """
        Nacita nastavenie pre skorovanie items kategorii

        Struktura:
            {0: {'category': 'running',
                 'group0': 'Apparel',
                 'group1': 'T-Shirts',
                 'group2': 'All',
                 'settings': 'DECREASE'},
             
             1: {'category': 'All',
                 'group0': 'All',
                 'group1': 'All',
                 'group2': 'Polo',
                 'settings': 'INCREASE'},
             ...
            }
        """

        df_pricing_groups_settings = gapi.google_sheet2df(sample_spreadsheet_id, 'pricing_groups_settings')
        if df_pricing_groups_settings is None:
            self.pricing_groups_settings = {}
        
        else:
            df_pricing_groups_settings = df_pricing_groups_settings.dropna().drop_duplicates(subset=['category','group0','group1','group2'], keep='last')
            self.pricing_groups_settings = df_pricing_groups_settings.to_dict(orient='index')
    
    @timeit
    def _load_discount_levels_override(self, gapi, sample_spreadsheet_id):
        """
        Nacita discount levels override 

        struktura:
            {
                ('HARD_SALE', 'nike', 'SK', 'football'): {'Season length (weeks)': 13.0,
                                                          'Discount Level 1': 0.0,
                                                          'Discount Level 2': 0.25,
                                                          'Discount Level 3': 0.35,
                                                          'Discount Level 4': 0.45,
                                                          'Discount Level 5': 0.55},
                ...
            }
        """
        df_discount_levels_override = gapi.google_sheet2df(sample_spreadsheet_id, 'discount_levels_override')

        if df_discount_levels_override is not None:
            discount_levels_override = discountLevels2dict(df_discount_levels_override, index=['Scoring type','Brand','Country','Category'])
        else:
            discount_levels_override = {}

        self.discount_levels_override = discount_levels_override
            
    @timeit
    def _load_orders(self, styles, from_date = None, to_date = None):
        """
        Nacita historiu objednavok
        
        Musi obsahovat polia:
            - style (str)
            - country_code (str)
            - date (datetime)
        """
        self.df_orders =  get_orders(
            styles = styles, 
            from_date = from_date,
            to_date = to_date
        )
        
        if self.df_orders.empty:
            raise Exception('Orders are empty!!!')
    
    @timeit
    def _load_quantities_in_inventory(self, styles):
        """
        Nacita stav skladu
        struktura: {'style_1': 10, 'style_2': 3}
        """
        
        # stav skladu k danemu dnu
        self.quantities_in_inventory = get_quantities_from_inventory(styles, as_dict=True, nth_latest=1)
        
        # stav skladu 7 dni dozadu
        self.quantities_in_inventory_7days = get_quantities_from_inventory(styles, as_dict=True, nth_latest=1)
        
        # vsetky unikatne styly z inv7 a inv30
        self.inventory_history_styles = set(self.quantities_in_inventory.keys()) | set(self.quantities_in_inventory_7days.keys())
        
    @timeit    
    def _load_price_history(self, country_competitors):
        """
        Nacita vsetky data z CompetitorsPriceHistory 
        """
        to_date = dt.date.today()
        from_date = to_date - dt.timedelta(days=3)
        
        with open(self.settings.google_service_account_json_path, "r") as f:
            credentials = json.load(f)
            
        df = load_competitors_data(credentials, from_date, to_date, 90)
        
        df['currency'] = df.apply(
            lambda row: COUNTRY_CODE_CURRENCY_MAPPER[row['country_code']] 
            if row['currency'] == '' 
            else row['currency'],
            axis=1
        )
        
        df['is_our_shop'] = df.apply(
            lambda row: is_our_shop(row['url'], row['competitor_shop_name'],['kickz']), 
            axis=1
        )
        
        df['is_important_competitor'] = df.apply(
            lambda row: is_important_competitor(row, country_competitors, 90),
            axis=1
        )
        
        
        # KONVERZIA LOKALNEJ CENY NA EURA !!!
        if not df.empty:
            df['price'] = df.apply(
                lambda row: row['price'] / self.conversion_rates[COUNTRY_CODE_CURRENCY_MAPPER[row['country_code']]], 
                axis=1
            )
        
        # treba nastavit kvol izachovanie rovnakej struktury ako s prisyncom
        df['in_stock'] = 1
        df['change_day'] = 0
                                                                            
        # posledna dostupna cena pre dany link
        df = df.sort_values(by=['url','date']).drop_duplicates(subset=['url'],keep='last')
    
        self.df_price_history = df.round(2)
    
    @timeit
    def _load_rcmnd_history(self, history_days = 6):
        cols = ['country_code','style','date','sell_power_week','price_original_currency','last_changed_days_ago']
        to_date = self.run_time.date()
        from_date = self.run_time.date() - dt.timedelta(days=history_days)
        
        try:
            df_rcmnd_history = S3RcmndHistory.load(from_date, to_date, columns=cols)
        except Exception as e:
            logger.warning(e)
            df_rcmnd_history = pd.DataFrame(columns=cols)
        
        df_rcmnd_history['date'] = pd.to_datetime(df_rcmnd_history['date'])
        self.df_rcmnd_history = df_rcmnd_history
        
    @timeit
    def _load_past_sell_power(self,history_days = 6):
        self.past_sell_power = self.df_rcmnd_history[
            self.df_rcmnd_history.date >= pd.Timestamp(self.run_time.date() - dt.timedelta(days=history_days))
        ]\
            .sort_values(['country_code','style','date'])[['country_code','style','date','sell_power_week']]\
            .groupby(['country_code','style']).last()\
            .fillna(np.nan)\
            .to_dict('index')
        
    @timeit
    def _load_last_changed_days_ago(self):
        self.last_changed_days_ago = self.df_rcmnd_history.sort_values(['style','date'])[['style','last_changed_days_ago']]\
            .groupby('style').last()\
            .fillna(0)\
            .to_dict()\
            .get('last_changed_days_ago')
        
    @timeit
    def _load_items_categories(self, styles):
        """
        Nacita kategorie, group0, group1, group2 z items 
        """
        self.items_categories = get_style_items_categories(styles, as_dict=True)
      
    @timeit
    def _load_prices_with_VAT(self):
        """
        Vrati ceny produktov
        struktura: {('style_1','country_code_1'): (price_EUR, price_from, base_price_EUR, price_original_currency), 
                    ('style_2','country_code_2'): (price_EUR, price_from, base_price_EUR, price_original_currency)}
                     
        """
        self.prices_with_VAT = get_prices_with_VAT(pricing_logic_data = self.__dict__)
        
    @timeit       
    def _load_data(self):
        logger.info('loading conversion rates...')
        self._load_conversion_rates()
        
        logger.info('loading data from google sheets...')
        self._load_data_from_google_sheets(
            sample_spreadsheet_id = self.settings.gs_spreadsheet_id,
            path_token = self.settings.gs_path_token,
            path_client_secret = self.settings.gs_path_client_secret
        )
         
        logger.info('loading data from google ads...')
        self._load_google_ads()
        
        logger.info('loading products_to_score data from S3')
        self._load_products_to_score()
        
        logger.info('loading orders...')
        self._load_orders(styles = self.styles, 
                          from_date = self.run_time.date() - dt.timedelta(190))
        
        logger.info('loading quantity in inventory...')
        self._load_quantities_in_inventory(styles = self.styles)
        
        logger.info('loading competitors data...')
        self._load_price_history(
            country_competitors = self.country_competitors
        )
        
        logger.info('loading our prices...')
        self._load_prices_with_VAT()
        
        logger.info('loading rcmnd history...')
        self._load_rcmnd_history(history_days = 6)
        
        logger.info('loading past sell power...')
        self._load_past_sell_power(history_days = 6)
        
        logger.info('loading last changed days ago...')
        self._load_last_changed_days_ago()
        
        logger.info('loading style items categories...')
        self._load_items_categories(styles = self.styles)
        
    @timeit
    def _get_data_from_discount_levels(self, country_code, category, brand, item_category, item_group0): 
        # custom logic for HARD_SALE
        if category == "HARD_SALE":
            if item_group0 == 'Footwear':
                category = 'HARD_SALE_FOOTWEAR'
            elif item_group0 == 'Apparel':
                category = 'HARD_SALE_APPAREL'
            else:
                category = 'HARD_SALE_ACCESSORIES'
                
        return (
            self.discount_levels_override.get((category, brand, country_code, item_category)) 
            if self.discount_levels_override.get((category, brand, country_code, item_category))
            else self.discount_levels.get(category, {}).get((brand, country_code))
        )
        
    @timeit
    def _get_product_demand(self, product_styles, timestamp_days = 7, country_codes = ['ALL']):
        
        # pocet predanych kusov za poslednych 'timestamp_days' dni
        this_week_demand = self._get_sold_items(styles = product_styles, 
                                                country_codes = country_codes, 
                                                last_x_days = timestamp_days)


        # pocet predanych kusov za od timestamp_days az timestamp_days*2 dozadu
        last_week_demand = (self._get_sold_items(styles = product_styles, 
                                                 country_codes = country_codes, 
                                                 last_x_days = timestamp_days*2) 
                            - this_week_demand)

        # indikator ci demand ide hore
        if last_week_demand == 0:
            if this_week_demand > 0:
                product_demand = this_week_demand
            else:
                product_demand = 0
        else:
            product_demand = this_week_demand / last_week_demand

        return product_demand
    
    @timeit
    def _get_sold_items(self, styles, country_codes = ['ALL'], last_x_days = None):
        """
        Vrati pocet predanych kusov za poslednych 'last_x_days' dni
        """
        
        # ked chceme pocet predanych kusov za cele dostupne obdobie
        if last_x_days is None or last_x_days > self.max_last_x_days:
            last_x_days = self.max_last_x_days
            
        
        sold_items = 0
        for style in styles:
            for country_code in country_codes:
                try:
                    sold_items += self.df_sold_items_history.at[(style, country_code, last_x_days), 'quantity']
                except:
                    pass
        
        return sold_items
    
    @timeit
    def _get_min_max_discount(self,data):
        min_discount = 0
        if not np.isnan(data['overriden_discount']): # ak niekto prepisal zlavu
            max_discount = data['overriden_discount']
        
        elif (
                (data['category'] == 'ST') 
             or (data['category'] == 'HARD_SALE') 
             or (data['category'] == 'SOFT_SALE')
             or (data['category'] == 'ENTRY_SALE')
        ):
            min_discount = data['min_discount_ST']
            max_discount = data['max_discount_ST']
        
        elif data['category'] == 'IMP':
            max_discount = self.brand_discount.get((data['brand'],data['country_code']),0)
        
        elif data['category'] == 'TEAM_SALE':
            max_discount = self.team_sale_discounts.get((data['brand'], data['country_code']), self.team_sale_discounts.get(data['brand'], {})).get('max_discount', 0)
            min_discount = self.team_sale_discounts.get((data['brand'], data['country_code']), self.team_sale_discounts.get(data['brand'], {})).get('min_discount', 0)
            
        elif data['category'] == 'DROPSHIPMENT':
            max_discount = self.dropshipment_discounts.get((data['brand'], data['country_code']), self.dropshipment_discounts.get(data['brand'], {})).get('max_discount', 0)
            min_discount = self.dropshipment_discounts.get((data['brand'], data['country_code']), self.dropshipment_discounts.get(data['brand'], {})).get('min_discount', 0)
        
        elif data['category'] == 'CARRYOVERS':
            max_discount = self.carryovers_discounts.get((data['brand'], data['country_code']), self.carryovers_discounts.get(data['brand'], {})).get('max_discount', 0)
            min_discount = self.carryovers_discounts.get((data['brand'], data['country_code']), self.carryovers_discounts.get(data['brand'], {})).get('min_discount', 0)
        
        elif data['category'] == 'TEAMSPORT_OVERSTOCK':
            max_discount = self.teamsport_overstock_discounts.get((data['brand'], data['country_code']), self.teamsport_overstock_discounts.get(data['brand'], {})).get('max_discount', 0)
            min_discount = self.teamsport_overstock_discounts.get((data['brand'], data['country_code']), self.teamsport_overstock_discounts.get(data['brand'], {})).get('min_discount', 0)
        
        elif data['category'] == 'TOTAL_CLEARANCE':
            max_discount = self.total_clearance_discounts.get((data['brand'], data['country_code']), self.total_clearance_discounts.get(data['brand'], {})).get('max_discount', 0)
            min_discount = self.total_clearance_discounts.get((data['brand'], data['country_code']), self.total_clearance_discounts.get(data['brand'], {})).get('min_discount', 0)
            
        elif data['category'] == 'INDOOR_SHOES':
            max_discount = self.indoor_shoes_discounts.get((data['brand'], data['country_code']), self.indoor_shoes_discounts.get(data['brand'], {})).get('max_discount', 0)
            min_discount = self.indoor_shoes_discounts.get((data['brand'], data['country_code']), self.indoor_shoes_discounts.get(data['brand'], {})).get('min_discount', 0)
        
        elif data['category'] == 'DESTROY_COMPETITORS':
            max_discount = self.destroy_competitors_discount.get((data['style'],data['country_code']), 0)
        
        else:
            max_discount = 0
        
        return min_discount,max_discount
      
    @timeit
    def _get_demand_key_and_group_logic(self, style, category, group0, group1, group2):
        scores_mapper = {
            'group2': 8,
            'group1': 4,
            'group0': 2,
            'category': 1,
            'All': 0.5
        }
        
        demand_key = style # default demand key is style
        demand_key_original = style
        group_logic = 'OFF' # defalut group logic is OFF => separated scoring
        
        # create dictionary from inputs
        style_info = {
            'category': category,
            'group0': group0,
            'group1': group1,
            'group2': group2
        }

        filter_scores = {}
        for index, filter_ in self.pricing_groups_settings.items():
            filter_score = 0
            filter_min_score = 0
            
            for col in style_info.keys():
                if style_info[col] == filter_[col]:
                    filter_min_score += scores_mapper[col]
                    score_col = col
                elif filter_[col] == "All":
                    filter_min_score += scores_mapper['All']
                    score_col = 'All'
                else:
                    filter_min_score += scores_mapper[col]
                    continue
                
                filter_score += scores_mapper[score_col]
            
            if filter_min_score <= filter_score:
                filter_scores[index] = filter_score
        
        if filter_scores:
            best_index, best_score = sorted(list(filter_scores.items()), key=lambda x: x[1], reverse=True)[0]
        
            demand_key = best_index
            demand_key_original = self.pricing_groups_settings[best_index]
            group_logic = self.pricing_groups_settings[best_index]['settings']
            
                
        return demand_key, demand_key_original, group_logic
    
    @timeit
    def _get_competitors_comparison(self, product_name, country_code, prefix='product'):
        default = {}
        default['count_all_competitors'] = 0
        default['count_important_competitors'] = 0
        default['all_competitors_list'] = []
        default['all_competitors_links'] = []
        default['all_competitors_prices'] = []
        default['all_competitors_in_stock'] = []
        default['all_competitors_price_change_day'] = []
        default['important_competitors_list'] = []
        default['important_competitors_links'] = []
        default['important_competitors_prices'] = []
        default['important_competitors_in_stock'] = []
        default['important_competitors_price_change_day'] = []
        
        return {
            f'{prefix}_{key}': value for key,value in 
            self.competitors_comparison.get((product_name, country_code), default).items()
        }
    
    @timeit
    def _get_season_length_and_days_from_season_start(self, style, product, country_code, category, brand, item_category, item_group0):
        """
        Vrati dlzku sezony v tyzdnoch, a datum od startu sezony
        """
        brand_discount_settings = self._get_data_from_discount_levels(country_code, category, brand, item_category, item_group0)
        style_season_length_override = self.style_season_length_override.get(style,{}).get(country_code)
        if not brand_discount_settings:
            season_length = np.nan
            days_from_season_start = np.nan       
        
        else:
            # datum prvej objednavky PRODUKTU
            first_product_order = self.first_product_order.get(product, dt.date(2000,1,1))
            season_length = style_season_length_override if style_season_length_override else brand_discount_settings['Season length (weeks)']
            season_start = self.run_time.date() - dt.timedelta(days=season_length*7)
            days_from_season_start = (self.run_time.date() - max(first_product_order, season_start)).days
        
        return season_length, days_from_season_start
    
    @timeit
    def _get_ST_data(self, product, brand, country_code, style, category, item_category, item_group0):
        """
        Vytvori data pre vypocet ST a sell power
        """
        season_length, days_from_season_start = self._get_season_length_and_days_from_season_start(style, product, country_code, category, brand, item_category, item_group0)
        quantity_in_inventory = self.quantities_in_inventory.get((brand, style), np.nan)
        
        if item_category is None:
            item_category = 'unknown'
            
        ST_setting = self.st_settings.get((country_code, item_category), {}).get('setting')
        ST_rate_pct = self.st_settings.get((country_code, item_category), {}).get('rate_pct', 100)
        
        if ST_setting == 'COUNTRY':
            sold_items_today = self._get_sold_items([style], country_codes=[country_code], last_x_days = 0)
            sold_items_7_days = self._get_sold_items([style], country_codes=[country_code], last_x_days = 7)
            sold_items_season = self._get_sold_items([style], country_codes=[country_code], last_x_days = days_from_season_start)       
        else:
            sold_items_today = self._get_sold_items([style], last_x_days = 0)
            sold_items_7_days = self._get_sold_items([style], last_x_days = 7)
            sold_items_season = self._get_sold_items([style], last_x_days = days_from_season_start)
            
        
        return {
            'season_length': season_length,
            'days_from_season_start': days_from_season_start,
            'quantity_in_inventory': quantity_in_inventory,
            'ST_setting': ST_setting,
            'ST_rate_pct': ST_rate_pct,
            'sold_items_today': sold_items_today,
            'sold_items_7_days': sold_items_7_days,
            'sold_items_season': sold_items_season  
        }
    
    @timeit
    def _get_sell_power_and_max_discount_ST(self, product, brand, country_code, style, 
                                            category, item_category, item_group0):
        # nastavenie zliav pre krajinu a znacku
        brand_discount_settings = self._get_data_from_discount_levels(country_code, category, brand, item_category, item_group0)
        
        if not brand_discount_settings:
            sell_through_week = np.nan
            sell_power_week = np.nan
            sell_through_day = np.nan
            sell_power_day = np.nan
            min_discount = 0
            max_discount = 0
            ST_setting = np.nan
            ST_rate_pct = np.nan
            discount_level = np.nan
        else:
            ST_data = self._get_ST_data(product, brand, country_code, style, category, item_category, item_group0)
            ST_setting = ST_data['ST_setting']
            ST_rate_pct = ST_data['ST_rate_pct']
            min_discount = brand_discount_settings['Discount Level 1']
            sell_through_day, sell_power_day = self._compute_ST_and_sell_power(ST_data['sold_items_today'], 
                                                                               ST_data['sold_items_season'], 
                                                                               ST_data['quantity_in_inventory'], 
                                                                               ST_data['season_length'])
            sell_through_week, sell_power_week = self._compute_ST_and_sell_power(ST_data['sold_items_7_days'], 
                                                                               ST_data['sold_items_season'], 
                                                                               ST_data['quantity_in_inventory'], 
                                                                               ST_data['season_length'])


            if np.isnan(sell_power_week):
                max_discount = brand_discount_settings['Discount Level 5']
                discount_level = np.nan
            elif sell_power_week > ST_data['ST_rate_pct']:
                max_discount = brand_discount_settings['Discount Level 1']
                discount_level = 1 
            elif sell_power_week > ST_data['ST_rate_pct'] * 0.65:
                max_discount = brand_discount_settings['Discount Level 2']
                discount_level = 2
            elif sell_power_week > ST_data['ST_rate_pct'] * 0.4:
                max_discount = brand_discount_settings['Discount Level 3']
                discount_level = 3
            elif sell_power_week > ST_data['ST_rate_pct'] * 0.01:
                max_discount = brand_discount_settings['Discount Level 4']
                discount_level = 4
            else:
                max_discount = brand_discount_settings['Discount Level 5']
                discount_level = 5
        
        data = {}
        data['sell_through_week'] = np.round(sell_through_week, 2)
        data['sell_power_week'] = np.round(sell_power_week, 2)
        data['sell_through_day'] = np.round(sell_through_day, 2)
        data['sell_power_day'] = np.round(sell_power_day, 2)
        data['max_discount_ST'] = max_discount
        data['min_discount_ST'] = min_discount
        data['ST_setting'] = ST_setting
        data['ST_rate_pct'] = ST_rate_pct
        data['ST_discount_level'] = discount_level
    
        return data
    
    @timeit
    def _get_category(self, style, country_code, quantity_in_inventory):
        original_category = self.style_category.get(style)
        
        if (style, country_code) in self.destroy_competitors_discount.keys():
            return 'DESTROY_COMPETITORS'
        elif original_category == 'ST' and quantity_in_inventory <= 5:
            return 'IMP'
        
        return original_category
        
    @timeit
    def _compute_sold_items(self):
        """
        Pre kazdy styl spocita pocet predanych kusov za poslednych x dni
        Vrati dataframe kde index je style a last_x_days
        
        Priklad:
            df_sold_items_history.at[('000544-bk-01', 'DE', 30), 'quantity'] 
            vrati pocet predanych kusov za poslednych 30 dni v DE
        """
        today = self.run_time.date()
        df_orders = self.df_orders[['date','style','country_code','quantity']]
        
        max_last_x_days = (df_orders['date'].max() - df_orders['date'].min()).days
        if np.isnan(max_last_x_days):
            max_last_x_days  = 0

        sold_items_history = []
        for days in range(0, max_last_x_days + 1):
            from_date = today - dt.timedelta(days=days)
            df_orders_days = df_orders[df_orders['date'] >= pd.Timestamp(from_date)]
            
            df_orders_grouped = df_orders_days.groupby(['style','country_code'])[['quantity']].sum()
            df_orders_grouped['last_x_days'] = days
            
            sold_items_history.append(df_orders_grouped)
        
        # pocet predanych kusov podla krajin
        sold_items_history_countries = pd.concat(sold_items_history)\
                                         .reset_index()
        
        
        if not sold_items_history_countries.empty:
            # pocet predanych kusov vo vsetkych krajinach dokopy
            sold_items_history_total = sold_items_history_countries.groupby(['style','last_x_days'],
                                                                            as_index=False)[['quantity']]\
                                                                   .sum()
            sold_items_history_total['country_code'] = 'ALL'
            
            df_sold_items_history = pd.concat([sold_items_history_countries, sold_items_history_total])\
                                      .set_index(['style','country_code','last_x_days'])
        else:
            df_sold_items_history = pd.DataFrame()
        
        self.max_last_x_days = max_last_x_days
        self.df_sold_items_history = df_sold_items_history
    
    @timeit
    def _compupte_ads_attributes(self, country_code, style):
        # google ads z minuleho dna
        ads_style_info = self.gapi_yesterday_products_ads_dict.get(country_code,{}).get(style)
        
        if ads_style_info:
            ads_clicks = float(ads_style_info.get('clicks'))  
            ads_ctr = float(ads_style_info.get('ctr'))
            ads_impressions = float(ads_style_info.get('impressions'))
        else:
            ads_clicks = np.nan
            ads_ctr = np.nan
            ads_impressions = np.nan
            
        return ads_clicks, ads_ctr, ads_impressions
    
    @timeit
    def _compute_first_product_order(self):
        """
        Pre kazdy produkt najde datum prveho predaja z orders
        
        Struktura:
            {'adidas  condivo 18 cotton': datetime.date(2020, 10, 31),
             'adidas  ever pro': datetime.date(2020, 10, 17),
             'adidas  everclub': datetime.date(2020, 10, 17),
             'adidas  parma 16': datetime.date(2020, 10, 15),
        """
        
        df_orders = self.df_orders
        df_orders['product_name'] = df_orders['style'].apply(lambda style: self.styles_prods_mapper.get(style))
        df_orders['date'] = df_orders['date'].dt.date
        
        self.first_product_order = df_orders.groupby('product_name')[['date']].min().to_dict().get('date')
    
    @timeit
    def _compute_style_latest_purchase_cost(self):
        """
        Pre kazdy styl najde poslednu nakupnu cenu 
        """
        df_orders = self.df_orders
        
        self.latest_purchase_price = {}
        # self.latest_purchase_price = df_orders.sort_values(['country_code','style','date'])\
        #                                       .groupby(['country_code','style'])[['unit_cogs']].last()\
        #                                       .round(2)\
        #                                       .to_dict().get('unit_cogs')
        
    @timeit
    def _compute_is_new_product(self, style, new_first_x_days=14):
        """
        Vrati ci styl je novy. 
        Novy je ak rozdiel datumu behu a datumu pridania v dnoch je mensi ako new_first_x_days
        """
        # datum kedy bol produkt pridany
        date_added = self.date_added_mapper.get(style, dt.date(2021,1,1))
        
        # kolko dni ho povazujeme za novy
        is_new_days = self.wait_after_release.get(style, new_first_x_days)
        
        if (self.run_time - date_added.to_pydatetime()).days < is_new_days:
            return True
        return False

    @timeit
    def _compute_competitors_comparison(self,min_price=0,max_price=1000):
        def product_competitors_summary(df):
            important_competitors_prices = df[df['is_important_competitor'] == True]
            rest_competitors_prices = df[df['is_important_competitor'] == False]

            data = {}
            data['count_all_competitors'] = df['competitor_shop_name'].nunique()
            data['count_important_competitors'] = important_competitors_prices['competitor_shop_name'].nunique()

            data['all_competitors_list'] = df['competitor_shop_name'].unique().tolist()
            data['all_competitors_links'] = df['url'].tolist()
            data['all_competitors_prices'] = df['price'].tolist()
            data['all_competitors_in_stock'] = df['in_stock'].tolist()
            data['all_competitors_price_change_day'] = df['change_day'].astype(float).tolist()

            data['important_competitors_list'] = important_competitors_prices['competitor_shop_name'].unique().tolist()
            data['important_competitors_links'] = important_competitors_prices['url'].tolist()
            data['important_competitors_prices'] = important_competitors_prices['price'].tolist()
            data['important_competitors_in_stock'] = important_competitors_prices['in_stock'].tolist()
            data['important_competitors_price_change_day'] = important_competitors_prices['change_day'].astype(float).tolist()

            return data
        
        df_price_history = self.df_price_history
        df_price_history['base_price'] = df_price_history.apply(
                lambda row: self.prices_with_VAT.get((row['style'], row['country_code']), {}).get('base_price_EUR', np.nan),axis=1
        )
        df_price_history['min_price_threshold'] = df_price_history['base_price'].apply(
            lambda base_price: min_price if np.isnan(base_price) else base_price * 0.5
        )
        df_price_history['max_price_threshold'] = df_price_history['base_price'].apply(
            lambda base_price: max_price if np.isnan(base_price) else base_price * 2
        )

        df_price_history = df_price_history[
                (df_price_history['price'] > df_price_history['min_price_threshold'])
              & (df_price_history['price'] <= df_price_history['max_price_threshold'])
              & (df_price_history['is_our_shop'] == False)
        ]
        
        self.competitors_comparison = df_price_history.groupby(['style','country_code'])\
                                                      .apply(product_competitors_summary)\
                                                      .to_frame('data')\
                                                      .to_dict('dict')\
                                                      .get('data')
    
    @timeit
    def _compute_diff_to_expected_margin(self, style, country_code):
        """
        Spocita ci je sucasna cena pod hranicou ocakavanej marze
        """
        current_price = self.prices_with_VAT.get((style, country_code),{}).get('price_EUR',np.nan)
        purchase_price = self.latest_purchase_price.get((country_code, style), np.nan)
        expected_margin = self.margin_settings.get(country_code, {}).get('target_margin', np.nan)
        
        try:
            margin_pct = (current_price - purchase_price) / current_price * 100 
        except ZeroDivisionError:
            margin_pct = np.nan
        
        
        return margin_pct - expected_margin
    
    @timeit
    def _compute_ST_and_sell_power(self, sold_items, sold_items_season, quantity_in_inventory, season_length):
        """
        Vypocita ST a sell power
        """
        try:
            st = sold_items / (sold_items_season + quantity_in_inventory) * 100
        except ZeroDivisionError:
            st = 0
            
        sell_power = st * season_length
        
        return st,sell_power
    
    @timeit 
    def _compute_gapi_714_ratios(self):
        """
        Vypocita ctr, impresion, clicks ratio aj cez kategorie
        """
        df_categories = pd.DataFrame.from_dict(self.items_categories, orient='index')\
                                    .reset_index()\
                                    .rename(columns={'index': 'style'})
        
        df_gads = self.df_gapi_714_ratios.merge(df_categories, on='style', how='left')
        
        # remove infinite values
        df_gads = df_gads[(~df_gads['clicks_ratio'].isin([-np.inf, np.inf]))
                        & (~df_gads['impresions_ratio'].isin([-np.inf, np.inf]))
                        & (~df_gads['ctr_ratio'].isin([-np.inf, np.inf]))]
        
        
        category_gads = []
        for index, category_filter in self.pricing_groups_settings.items():
            query_string = ' and '.join([f"item_{k} == '{v}'" for k, v in category_filter.items() if v not in ['All','INCREASE','DECREASE','KEEP','AUTO']])
            df_category_gads = df_gads.query(query_string).groupby('country_code')[['clicks_ratio','impresions_ratio','ctr_ratio']].mean()
            df_category_gads['style'] = index
            df_category_gads = df_category_gads.reset_index()
            
            category_gads.append(df_category_gads)

        if category_gads:
            df_all_category_gads = pd.concat(category_gads, ignore_index=True)
        else:
            df_all_category_gads = pd.DataFrame()
            
        df_gads_final = pd.concat(
            [
                df_gads[['country_code', 'clicks_ratio', 'impresions_ratio', 'ctr_ratio', 'style']],
                df_all_category_gads
            ],
            ignore_index=True
        )
            
        self.gapi_714_ratios = df_to_nested_dict(df_gads_final,'country_code', 'style', ['clicks_ratio','impresions_ratio','ctr_ratio'])
        
    @timeit
    def _create_data_for_pricing(self):
        data_for_pricing  = []
        
        for product in self.products:
            logger.debug(f'PRODUCT: {product}')
            
            # znacka 
            brand = self.product_brand_mapper[product].lower()
            
            # vsetky styly pre dany produkt
            product_styles = self.prods_styles[product]
            
            # indikator ci demand ide hore pre dany produkt 
            product_demand = self._get_product_demand(product_styles = product_styles, 
                                                      timestamp_days = 7)
            
            for country_code in self.country_codes:
                logger.debug(f'Checking country: {country_code}')
                
                # konkurencia pre produkt v danej krajine (bez ohladu na styl)
                data_product_competitors = self._get_competitors_comparison(product, country_code, prefix='product')
                
                for style in product_styles:
                    logger.debug(f'Checking style {style}')
                    data_style_competitors = self._get_competitors_comparison(style, country_code, prefix='style')
                    
                    # style demand 
                    # list country_codes kde je spusteny autopricing
                    scored_countries_country_codes = [country_code for country_code,scoring in self.score_style_in_country[style].items() if scoring]
                    style_demand = self._get_product_demand(product_styles = [style], 
                                                            timestamp_days = 7,
                                                            country_codes = scored_countries_country_codes)
                    
                    # pocet kusob na sklade
                    quantity_in_inventory = self.quantities_in_inventory.get((brand,style), np.nan)
                    quantity_in_inventory_7days = self.quantities_in_inventory_7days.get((brand,style), np.nan)
                    quantity_in_inventory_ratio = quantity_in_inventory / quantity_in_inventory_7days\
                                                  if quantity_in_inventory_7days != 0 else np.inf
                    
                    # kategoria 
                    category = self._get_category(style, country_code, quantity_in_inventory)
                    
                    # style item categories, groups
                    item_category = self.items_categories.get(style, {}).get('item_category')
                    item_group0 = self.items_categories.get(style, {}).get('item_group0')
                    item_group1 = self.items_categories.get(style, {}).get('item_group1')
                    item_group2 = self.items_categories.get(style, {}).get('item_group2')
                    
                    # dlzka sezony, pocet dni od zaciatku sezony 
                    season_length, days_from_season_start = self._get_season_length_and_days_from_season_start(style, product, country_code, category, brand, item_category, item_group0)
                    
                    # pocet predanych kusov z danneho stylu CELKOVO (nie iba v danej krajine)
                    sold_items = self._get_sold_items([style], last_x_days = None)
                    sold_items_today = self._get_sold_items([style], last_x_days = 0)
                    sold_items_7_days = self._get_sold_items([style], last_x_days = 7)
                    sold_items_14_days = self._get_sold_items([style], last_x_days = 14)
                    sold_items_season = self._get_sold_items([style], last_x_days = days_from_season_start)
                    
                    # demand key, group logic
                    demand_key, demand_key_original, group_logic = self._get_demand_key_and_group_logic(style, 
                                                                                   item_category, 
                                                                                   item_group0, 
                                                                                   item_group1, 
                                                                                   item_group2)
                
                    # pocet predanych kusov / stav skladu 
                    sold_inventory_7_ratio = sold_items_7_days / quantity_in_inventory_7days\
                                             if quantity_in_inventory_7days != 0 else np.inf
                    
                    # check if the style is new
                    is_new_style = self._compute_is_new_product(style, new_first_x_days=14)
                    
                    
                    # impressions demand
                    impressions_demand = self.gapi_714_ratios.get(country_code, {})\
                                                             .get(demand_key, {})\
                                                             .get('impresions_ratio', np.nan)
                    
                    # ctr demand
                    ctr_demand = self.gapi_714_ratios.get(country_code, {})\
                                                             .get(demand_key, {})\
                                                             .get('ctr_ratio', np.nan)
                    
                    # total demand
                    total_demand = np.mean([d for d in [impressions_demand, ctr_demand] if not np.isnan(d)])

                    ads_clicks, ads_ctr, ads_impressions = self._compupte_ads_attributes(country_code = country_code,
                                                                                         style = style)
                    
                    # ceny pre styl
                    prices = self.prices_with_VAT.get((style, country_code), {})
                    
                    # currency
                    currency = self.prices_with_VAT.get((style, country_code),{}).get('currency')
                    
                    # sell_power, max discount ST
                    sell_power_max_discount_ST = self._get_sell_power_and_max_discount_ST(product,brand, country_code, 
                                                                                          style, category, item_category, item_group0)
                    
                    # kolko percent sme nad alebo pod ocakavanou marzou
                    diff_to_expected_margin = self._compute_diff_to_expected_margin(style, country_code)
                    
                    # ak neskorujeme v danej krajine => master_switch = 0
                    # ak neskorujeme cely styl => master_switch = 0
                    master_switch = 0 if not self.score_style_in_country[style][country_code] or not self.master_switch.get(style,False) else 1
                    
                    data = {}
                    data['brand'] = brand
                    data['product_name'] = product
                    data['style'] =  style
                    data['price'] =  prices.get('price_EUR', np.nan)
                    data['price_from'] = prices.get('price_from', np.nan)
                    data['base_price'] = prices.get('base_price_EUR', np.nan)
                    data['price_original_currency'] = prices.get('price_local', np.nan)
                    data['category'] = category
                    data['country_code'] = country_code
                    data['product_demand'] = product_demand
                    data['style_demand'] = style_demand
                    data['impressions_demand'] = impressions_demand
                    data['ctr_demand'] = ctr_demand
                    data['total_demand'] = total_demand
                    data['total_sold_items'] = sold_items
                    data['sold_items_day'] = sold_items_today
                    data['sold_items_7_days'] = sold_items_7_days
                    data['sold_items_14_days'] = sold_items_14_days
                    data['sold_items_season'] = sold_items_season
                    data['sold_inventory_7_ratio'] = sold_inventory_7_ratio
                    data['quantity_in_inventory'] = quantity_in_inventory
                    data['quantity_in_inventory_7days'] = quantity_in_inventory_7days
                    data['quantity_in_inventory_ratio']= quantity_in_inventory_ratio
                    data['is_new_product'] = is_new_style
                    data['ads_clicks'] = ads_clicks
                    data['ads_ctr'] = ads_ctr
                    data['ads_impressions'] = ads_impressions
                    data['season_length'] = season_length
                    data['days_from_season_start'] = days_from_season_start
                    data['nodes_path'] = ''
                    data.update(data_product_competitors)
                    data.update(data_style_competitors)
                    data.update(sell_power_max_discount_ST)
                    data['last_day_sell_power_week'] = round(self.past_sell_power.get((data['country_code'], data['style']),{})\
                                                                                 .get('sell_power_week',np.nan),2)
                    data['overriden_discount'] = self.discount_override[style][country_code]
                    data['min_discount'], data['max_discount'] = self._get_min_max_discount(data)
                    data['last_changed_days_ago'] = self.last_changed_days_ago.get(style,0)
                    data['changed_last_days'] = True if self.changed_last_days_settings[style] > data['last_changed_days_ago'] else False
                    data['diff_to_expected_margin'] = diff_to_expected_margin
                    data['purchase_price'] = self.latest_purchase_price.get((country_code, style), np.nan)
                    data['expected_margin'] = self.margin_settings.get(country_code, {}).get('target_margin', np.nan)
                    data['expected_margin_use_in_country'] = self.margin_settings.get(country_code, {}).get('use_in_country', False)
                    data['master_switch'] = master_switch
                    data['item_category'] = item_category
                    data['item_group0'] = item_group0
                    data['item_group1'] = item_group1
                    data['item_group2'] = item_group2
                    data['demand_key'] = demand_key
                    data['demand_key_original'] = demand_key_original
                    data['group_logic'] = group_logic
                    
                    data_for_pricing.append(data)
  
        self.data_for_pricing = data_for_pricing
    
    @timeit
    def _write_to_production(self, df_recommendations, add_hours = 2):
        """
        Zapise recommendations do produkcnej db
        """
        
        df_recommendations = df_recommendations[
            (~df_recommendations['recom_price'].isnull())
          & (~df_recommendations['base_price'].isnull())
          & (df_recommendations['recom_change'] != 'NOT ENOUGH DATA')
          & (df_recommendations['master_switch'] == 1)
          & (df_recommendations['price'] != df_recommendations['recom_price'])
        ]
        
        # CHECK NA CHYBNE CENY 
        MAX_DISCOUNT = 0.1 # 1 - maximalna mozna zlava ktoru je mozne dat na produkt 
        df_recommendations = df_recommendations[
            (df_recommendations['recom_price'].between(
                df_recommendations['base_price'] * MAX_DISCOUNT, 
                df_recommendations['base_price'])
            )
          & (df_recommendations['base_price'] > 0)
        ]
        
        # currency mapper
        df_recommendations['currency'] = df_recommendations['country_code'].apply(lambda country_code: COUNTRY_CODE_CURRENCY_MAPPER[country_code])
        
        
        ### TEMP ###
        df_export_gs = df_recommendations[['country_code','brand','product_name','style', 'base_price','price', 'recom_price']]
        df_export_gs['recom_price'] = np.floor(df_export_gs['recom_price']) + 0.95
        
        sample_range = 'EXPORT!A2:ZZZ1000000'
        gapi = GoogleSheetsApi(
            path_token = self.settings.gs_path_token,
            path_client_secret = self.settings.gs_path_client_secret
        ) 
        logger.info(f'Updating data shape {df_export_gs.shape}...')
        logger.info(f'Deleting everything from {self.settings.gs_spreadsheet_id}...')
        gapi.delete_cell_values(
            sample_spredsheet_id = self.settings.gs_spreadsheet_id,
            sample_range_name = sample_range
        )
        logger.info(f'Inserting new values to {self.settings.gs_spreadsheet_id}...')
        gapi.update_cell_values(
            df = df_export_gs.replace(np.nan, ''),
            sample_spredsheet_id = self.settings.gs_spreadsheet_id,
            sample_range_name = sample_range,
            with_header = False
        )
        ### TEMP ###
        
        #### !!!!
        # Only UK and CH in local currencies
        df_recommendations.loc[~df_recommendations['currency'].isin(['CHF','GBP']), 'currency'] = 'EUR'
        #### !!!!
        
        # cena v lokalnej mene
        df_recommendations['recom_price_local_currency'] = df_recommendations.apply(lambda row: row['recom_price'] * self.conversion_rates[row['currency']], axis=1)
        
        # Kickz round to 0.95
        df_recommendations['recom_price_local_currency'] = np.floor(df_recommendations['recom_price_local_currency']) + 0.95

        # Loading material number mapper
        logger.info('loading material number mapper...')
        df_material_number = load_material_number_mapper()
        
        df_export = df_recommendations.merge(df_material_number, on=['brand','style'])
        df_export = df_export[df_export['material_number'] != 'Not Defined']
        
        # becuase of import to Netconomy
        df_export.loc[df_export['country_code'] == 'GB', 'country_code'] = 'UK' # becuase of import to Netconomy
        df_export['export_country_code'] = 'kickz-' + df_export['country_code']
        
        # export status
        df_export['export_status'] = 'DISCOUNT'
        
        # set up dates
        now = dt.datetime.now(dt.timezone.utc)
        df_export['export_from_date'] = now.strftime("%Y-%m-%dT%H:00:00")
        df_export['export_to_date'] = pd.to_datetime(dt.datetime(2100,1,1)).strftime("%Y-%m-%dT%H:00:00")
        
        #### TEMP ####
        df_export = df_export[
            (df_export['brand'].isin(['bucketz', 'new era'])) &
            ((df_export['base_price'] - df_export['recom_price']) > df_export['base_price'] * 0.05)
        ]
        #### TEMP ####
        
        df_export = df_export[
            [
                'material_number', 
                'recom_price_local_currency',
                'export_status',
                'export_from_date',
                'export_to_date',
                'export_country_code',
                'currency'
            ]
        ]
        
        df_export.to_parquet('ap_export.parquet')    
        
        upload_dataframe_to_azure_blob_storage(
            df_export,
            self.settings.export_container_name,
            self.settings.export_blob_name.format(ts_millis=int(now.timestamp() * 1000)),
            self.settings.export_connection_string
        )
        
    @timeit 
    def _kickz_find_optimal_prices(self):
        return find_optimal_prices(pricing_logic_data = self.__dict__)
        
        
    @timeit
    def _insert_into_s3(self):
        S3RcmndHistory.store(self.df_recommendations)
        
    @timeit
    def upload_dashboard_data(self):
        df_dashboard = self.df_recommendations
        df_dashboard['date'] = pd.to_datetime(df_dashboard['date']).dt.date
        
        df_dashboard = df_dashboard[
            ['date','country_code','brand','product_name','style',
             'base_price','price','recom_price'
             ,'ads_ctr', 'ads_impressions',
             'category','item_category','item_group0','item_group1','item_group2'
        ]].round(2)
        
        df_orders  = get_orders(from_date=df_dashboard['date'].iloc[0])
        df_orders['date'] = pd.to_datetime(df_orders['date']).dt.date
        
        df_dashboard = df_dashboard.merge(
            df_orders[['date','style','brand','quantity','country_code']].rename(columns={'quantity': 'sold_items_day'}),
            on = ['date', 'brand','style','country_code'],
            how = 'left'
        )
        df_dashboard['sold_items_day'] = df_dashboard['sold_items_day'].fillna(0)
        
        upload_dataframe_to_azure_blob_storage(
            df = df_dashboard,
            container_name = 'kickz-autopricing',
            blob_name = f"import/{df_dashboard['date'].unique()[0].isoformat()}.csv",
            connection_string = self.settings.dashboard_export_connection_string,
            header=True
        )
        
        
    @timeit
    def _cretate_recommendations_backup(self, path, df_recommendations):
        df_backup = df_recommendations.copy()
        
        cols = df_backup.select_dtypes('object').columns.tolist()
        df_backup[cols] = df_backup[cols].astype(str)
        
        df_backup.to_parquet(
            path = path, 
            compression = 'gzip', 
            index = False
        )
        
    @timeit   
    def run(self, insert_into_production=False, insert_into_s3=False):
        logger.info('pricing algo started...')
        self.run_time = (self._get_current_time() - dt.timedelta(days=1)).replace(hour=23, minute=59)
        
        logger.info('loading data started...')
        self._load_data()
        
        logger.info('computing sold items...')
        self._compute_sold_items()
        
        logger.info('computing latest purchase cost...')
        self._compute_style_latest_purchase_cost()
        
        logger.info('computing first products order...')
        self._compute_first_product_order()
        
        logger.info('computing competitors comparison...')
        self._compute_competitors_comparison(min_price=0,max_price=1000)
        
        logger.info('computing gapi_714_ratios...')
        self._compute_gapi_714_ratios()
        
        logger.info('creating data for pricing...')
        self._create_data_for_pricing()
        
        logger.info('searching for optimal prices...')
        self.df_recommendations = self._kickz_find_optimal_prices()

        
        logger.info('creating backup file...')
        self._cretate_recommendations_backup(
            path = f"backup/df_recommendations_{self.run_time.strftime('%Y%m%d')}.parquet",
            df_recommendations = self.df_recommendations
        )
        
        
        if insert_into_s3:
            logger.info('Inserting into s3...')
            self._insert_into_s3()
        
        if insert_into_production:
            logger.info('inserting into production database...')
            self._write_to_production(
                df_recommendations = self.df_recommendations, 
                add_hours = 0
            )
        
        logger.info('uploading dashboard data...')
        self.upload_dashboard_data()
        
        logger.info('pricing algo finished succesfully...')