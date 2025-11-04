import pandas as pd
import numpy as np
import re
import requests
import xmltodict
import logging
from io import StringIO
from fuzzywuzzy import fuzz
from ast import literal_eval

from libs.utils import retry
from azure.storage.blob import BlobServiceClient


logger = logging.getLogger(__name__)

COUNTRY_CODE_CURRENCY_MAPPER = {
    'CZ': 'CZK',
    'SK': 'EUR',
    'DE': 'EUR',
    'ES': 'EUR',
    'FR': 'EUR',
    'RO': 'RON',
    'HU': 'HUF',
    'IT': 'EUR',
    'AT': 'EUR',
    'HR': 'EUR',
    'NL': 'EUR',
    'BE': 'EUR',
    'DK': 'DKK',
    'SE': 'SEK',
    'IE': 'EUR',
    'PL': 'PLN',
    'PT': 'EUR',
    'FI': 'EUR',
    'SI': 'EUR',
    'BG': 'BGN',
    'GR': 'EUR',
    'EU': 'EUR',
    'NO': 'NOK', 
    'CH': 'CHF',  
    'GB': 'GBP',  
}

DOMAIN_COUNTRY_CODE_MAPPER = {
    '.cz': 'CZ',
    '.sk': 'SK', 
    '.de': 'DE',
    '.es': 'ES',
    '.fr': 'FR',
    '.ro': 'RO',
    '.hu': 'HU',
    '.it': 'IT',
    '.at': 'AT',
    '.hr': 'HR',
    '.nl': 'NL',
    '.be': 'BE',
    '.dk': 'DK',
    '.se': 'SE',
    '.ie': 'IE',
    '.pl': 'PL',
    '.pt': 'PT',
    '.fi': 'FI',
    '.si': 'SI',
    '.bg': 'BG',
    '.gr': 'GR',
    '.com': 'EU',
    '.no': 'NO',  
    '.ch': 'CH',  
    '.uk': 'GB', 
}

BRANDS = ['north face','goldbee','erima','puma','soccer supplement','compressport',
          'power system','under armour','new balance','vans','top4football',
          'top4running','topforsport','safety skin','gym glamour','tapedesign',
          'on running','smellwell','derbystar','ledlenser','converse','uhlsport',
          'spalding','hartmann','skinners','saucony','salomon','diadora','isostar',
          'new era','maurten','trusox','inov-8','cosmos','reebok','brooks',
          'nathan','saysky','jordan','mizuno','reusch','garmin', 'adidas originals','adidas','g-form','oakley',
          'suunto','ciele','lotto','asics','petzl','kempa','craft','umbro',
          'esio','nike','sony','jako','fila','cr7','cep','gym','stance']

def get_country_code_from_url(url, default_country_code = 'NOT FOUND'):
    """
    Vrati country code pre zadanu url. 
    www.nike.sk => 'SK'
    """
    
    domain = url[url.rfind('.'):]
    country_code = DOMAIN_COUNTRY_CODE_MAPPER.get(domain)
    
    if not country_code:
        country_code = default_country_code
        
    return country_code


def countryCompetitors2dict(df_competitors):
    country_competitors = {
        country.upper() : df_competitors[country].dropna().str.strip().str.lower().tolist() 
        for country in df_competitors.dropna(how='all',axis=1).columns
    }
    
    return country_competitors


def productsStyles2dict(df):
    prods_styles = {
        product_name.lower() : df.loc[df['product_name'] == product_name, 'style'].str.strip().str.lower().tolist()
        for product_name in df['product_name'].unique()
    }
    
    return prods_styles

def stylesCategory2dict(df):
    return df.drop_duplicates(subset=['style'], keep = 'last')[['style','category']]\
             .set_index('style')\
             .to_dict('dict')\
             .get('category')


def stylesDiscounts2dict(df):
    discount_cols = [col for col in  df.columns if 'discount' in col]
    
    df.drop_duplicates('style', keep='last', inplace=True)
    df[discount_cols] = df[discount_cols].replace('', np.nan).astype(float).divide(100)
    df['style'] = df['style'].str.lower()
    
    discount_dct = df.set_index('style')[discount_cols].rename(columns = {col: col.split('__')[0].strip() for col in discount_cols}).to_dict('index')
    
    return discount_dct

def stylesAutoPricing2dict(df):
    autopricing_cols = [col for col in  df.columns if 'auto_pricing' in col]
    
    df.drop_duplicates('style', keep='last', inplace=True)
    df[autopricing_cols] = df[autopricing_cols].astype(int).astype(bool)
    df['style'] = df['style'].str.lower()

    autopricing_dct = df.set_index('style')[autopricing_cols].rename(columns = {col: col.split('__')[0].strip() for col in autopricing_cols}).to_dict('index')
    
    return autopricing_dct


def changedLastDays2dict(df):
    df['DAYS'] = df['DAYS'].astype(int)
    
    return df.set_index('COUNTRY CODE').to_dict(orient='dict').get('DAYS')


def waitAfterRelease2dict(df):
    df['wait_after_release'] = df['wait_after_release'].astype(int)
    
    return df.drop_duplicates(subset=['style'], keep = 'last')[['style','wait_after_release']]\
             .set_index('style')\
             .to_dict('dict')\
             .get('wait_after_release')


def discountLevels2dict(df, index=['Brand','Country']):
    df['Brand'] = df['Brand'].str.lower().str.strip()
    df = df.drop_duplicates(subset=index, keep='last')\
           .set_index(index)\
           .astype(float)
    
    discount_cols = [col for col in df.columns if 'Discount Level' in col]
    df[discount_cols] = df[discount_cols].divide(100) # konverzia z percent na desatine cisla
    
    return df.dropna().to_dict('index')

def minMaxDisctount2dict(df):
    df['brand'] = df['brand'].str.lower().str.strip()
    df['min_discount'] = df['min_discount'].astype(float).divide(100) # konverzia z percent na desatine cisla
    df['max_discount'] = df['max_discount'].astype(float).divide(100) # konverzia z percent na desatine cisla
    
    if 'country' in df.columns:
        brand_country_disounts_dct = df.dropna().drop_duplicates(['brand', 'country']).set_index(['brand', 'country']).to_dict('index')
        brand_discounts_dct = df[df['country'].isna()].drop('country',axis=1).set_index('brand').to_dict('index')
        
        return brand_country_disounts_dct | brand_discounts_dct
    
    return df.set_index('brand').to_dict('index')

def is_exact_search(row, threshold = 95):
    link = re.sub('\W',' ', row['link'].lower())
    title = re.sub('\W',' ', row['title'].lower())
    query = row['search_query'].lower()
    
    if (fuzz.token_set_ratio(link, query) > threshold) or (fuzz.token_set_ratio(title, query) > threshold):
        return True
    return False


def is_our_shop(url, shop_name, shops_lst):
    for s in shops_lst:
        if (s in url) or (s in shop_name) :
            return True
    return False


def get_conversion_rates():
    rates = {'EUR': 1}
    try:
        r = requests.get('https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml')
        response_dict = xmltodict.parse(r.content)
        
        rates_list = response_dict['gesmes:Envelope']['Cube']['Cube']['Cube']
        for row in rates_list:
            rates[row['@currency']] = float(row['@rate'])
        
    except Exception as e:
        logger.exception('Exception occured')
    
    return rates


def clean_country_competitors(country_competitors_dict):
    """
    Ocisti konkurenciu
    
    Example:
        'store.nike.com' => 'nike'
        'https://www.runningpro.sk/' => 'runningpro'
    """
    
    def root_site(text):
        clean = text.lower()\
                    .strip()\
                    .replace('https:','')\
                    .replace('http:','')\
                    .replace('//','')\
                    .replace('www','')\
                    .split('.')
        
        if len(clean) > 1:
            return clean[-2]
        
        return clean[-1]
    
    country_competitors_clean = {}
    for country_code in country_competitors_dict:
        country_competitors_clean[country_code]  = [root_site(comp) for comp in country_competitors_dict[country_code]]

    return country_competitors_clean

def is_important_competitor(row, country_competitors, threshold=90):
    shop_name = row['competitor_shop_name']
    country_code = row['country_code']
    
    competitors = country_competitors.get(country_code,[])
    
    for competitor in competitors:
        if fuzz.partial_token_set_ratio(competitor, shop_name) > threshold:
            return True
    
    return False


def get_brand(product_name, brands=BRANDS):
    for brand in brands:
        if brand.lower() in product_name.lower():
            return brand.capitalize()
    
    return product_name.lower()\
                       .replace('wmns','')\
                       .strip()\
                       .split(' ')[0]\
                       .strip()\
                       .capitalize()

def excel_col_index_to_string(n, zero_based_index=True):
    """
    Vrati nazov stplca v exceli z jedho indexu
    Priklad:
    0 => 'A',
    1 => 'B',
    25 => 'Z'
    26 => 'AA'
    50 => 'AY'
    """
    string = ""
    if zero_based_index:
        n += 1 
    
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def safe_literal_eval(value):
    try:
        value = literal_eval(value)
    except:
        pass
    return value

def df_to_nested_dict(
    df: pd.DataFrame,
    group_col: str,
    nested_col: str,
    value_cols: list[str]
) -> dict:
    """
    Convert a DataFrame into a nested dictionary grouped by a specified column.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame.
    group_col : str
        Column name to group by (outer dictionary key).
    nested_col : str
        Column name to nest by (inner dictionary key).
    value_cols : list of str
        List of columns to include as values in the innermost dictionary.

    Returns
    -------
    dict
        Nested dictionary of the form:
        {
            group_col_value: {
                nested_col_value: {
                    value_col1: ...,
                    value_col2: ...,
                    ...
                }
            }
        }
    """
    return {
        group: {
            nested: {col: row[col] for col in value_cols}
            for nested, row in group_df.set_index(nested_col).iterrows()
        }
        for group, group_df in df.groupby(group_col)
    }


@retry(Exception, total_tries=5, initial_wait=60, backoff_factor=2, logger=logger)   
def upload_dataframe_to_azure_blob_storage(df, container_name, blob_name, connection_string, header=False):
    csv_data = df.to_csv(index=False, header=header, sep=';')
    
    # Create blob client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    # Upload CSV
    blob_client.upload_blob(csv_data, overwrite=True)
    
    logger.info(f"✅ DataFrame uploaded successfully to '{blob_name}' in container '{container_name}'")
