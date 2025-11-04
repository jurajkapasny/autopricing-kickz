import logging
import pyodbc
import pandas as pd
import datetime as dt
import numpy as np

from libs.s3 import S3
from libs.bq import BigQuery 

logger = logging.getLogger(__name__)

CONNECTION_STRING = """
    Driver={ODBC Driver 17 for SQL Server};
    Server=tcp:sql-one11-001.database.windows.net,1433;
    Database=sqldb-one11-001;
    Uid=predator;
    Pwd={mundiaL33};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30')
"""

def load_material_number_mapper():
    SQL = """
        SELECT   
            DISTINCT
            LOWER(TRIM(brand)) AS brand,
            LOWER(TRIM(style_id)) AS style,
            TRIM(material_number) AS material_number
        FROM [rawone11].[v_pim_articles_data]
        WHERE item_shop_active_kickz = 1
            AND name IS NOT NULL
            AND style_id IS NOT NULL
            AND brand IS NOT NULL;
    """
    with pyodbc.connect(CONNECTION_STRING) as con:
        df = pd.read_sql(SQL,con)
    
    if df.empty:
        logger.info('Table is empty!!!')
        
    return df

def load_competitors_data(credentials, from_date, to_date, threshold):
    """
    Load competitor pricing data from BigQuery within a given date range 
    and filtered by a matching threshold.

    Args:
        credentials (dict or str): JSON service account credentials used 
            to authenticate with BigQuery.
        from_date (str): Start date (inclusive) of the query in format 
            'YYYY-MM-DD'.
        to_date (str): End date (inclusive) of the query in format 
            'YYYY-MM-DD'.
        threshold (float): Minimum similarity threshold; only rows where 
            at least one of the matching fields exceeds this value are 
            included.

    Returns:
        pandas.DataFrame
    """
    bq = BigQuery.from_json_credentials(credentials)
    
    SQL = f"""
        SELECT 
            date,
            UPPER(country_code) AS country_code,
            brand,
            style,
            UPPER(currency) AS currency,
            price,
            LOWER(seller) AS competitor_shop_name,
            link AS url
        FROM dbt_eas_kickz_scraping.reporting_competitor_prices
        WHERE 
        (
            (query_inside_link > {threshold}) OR
            (product_name_inside_link > {threshold}) OR
            (style_inside_link > {threshold}) OR
            (query_inside_title > {threshold}) OR
            (product_name_inside_title > {threshold}) OR
            (style_inside_title > {threshold})
        )
        AND 
        (date BETWEEN '{from_date}' AND '{to_date}')
     """
    
    df = bq.get_data_from_query(SQL)
    
    if df.empty:
        logger.info('Table is empty!!!')
        
    return df.dropna()

def get_all_products() -> pd.DataFrame:
    """
    Retrieve all products from the database for Kickz.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with the following columns:
        - brand (str): The product brand, in lowercase.
        - product_name (str): The cleaned product name.
        - style (str): The generic material number (style), in lowercase.
    """
    
    SQL = """
        SELECT
            DISTINCT
                LOWER(TRIM(brand)) AS brand,
                LTRIM(RTRIM(LOWER(TRIM(brand)) + ' ' + REPLACE(LOWER(TRIM(name)), LOWER(TRIM(brand)), ''))) AS product_name,
                LOWER(TRIM(style_id)) AS style
        FROM [rawone11].[v_pim_articles_data]
        WHERE item_shop_active_kickz = 1
        AND name IS NOT NULL
        AND style_id IS NOT NULL
        AND brand IS NOT NULL;
    """

    with pyodbc.connect(CONNECTION_STRING) as con:
        df = pd.read_sql(SQL,con)
    
    if df.empty:
        logger.info('Table is empty!!!')
        
    return df

def get_quantities_from_inventory(styles=None, as_dict=False, nth_latest=1) -> pd.DataFrame:
    """
    Retrieve product quantities from inventory for a given balance date snapshot.

    Parameters
    ----------
    styles : list of str, optional
        If provided, filter the result to include only these style identifiers.
    as_dict : bool, default False
        If True, return the result as a dictionary mapping style → available quantity.
        If False, return a DataFrame.
    nth_latest : int, default 1
        Rank of the balance date snapshot to retrieve:
        - 1 = latest snapshot
        - 2 = second latest snapshot
        - N = N-th latest snapshot
    """
    SQL = f"""
        WITH date_ranked AS (
            SELECT DISTINCT
                sto.balance_date,
                DENSE_RANK() OVER (ORDER BY sto.balance_date DESC) AS date_rank
            FROM [raw11ts].[sage_stock_balance_history_per_style] AS sto
            INNER JOIN [one11].[v_warehouses] AS war
                ON war.warehouse_id = sto.warehouse_id
            WHERE war.entity_id = 1
              AND war.exclude = 0
        )
        SELECT
            sto.balance_date,
            LOWER(TRIM(sty.brand)) AS brand,
            LOWER(TRIM(sty.style_id)) AS style,
            SUM(sto.quantity) AS available_quantity
        FROM [raw11ts].[sage_stock_balance_history_per_style] AS sto
        INNER JOIN [one11].[v_warehouses] AS war
            ON war.warehouse_id = sto.warehouse_id
        LEFT JOIN product.one11_styles AS sty
            ON sty.id_style = sto.id_style
        WHERE war.entity_id = 1
          AND war.exclude = 0
          AND sto.balance_date = (
              SELECT balance_date
              FROM date_ranked
              WHERE date_rank = {nth_latest}
          )
        GROUP BY
            sto.balance_date,
            sty.brand,
            sty.style_id;
    """
    with pyodbc.connect(CONNECTION_STRING) as c:
        df = pd.read_sql(SQL,c)
    
    if styles:
        df = df[df['style'].isin(styles)]
    
    if df.empty:
        logger.info('Table is empty!!!')
    
    if as_dict:
        return df.set_index(['brand','style']).to_dict().get('available_quantity')
    
    return df

def get_live_styles() -> list:
    """
    Retrieve all active style codes

    Returns
    -------
    set of str
        Unique lowercase style codes.
    """
    SQL = """
        SELECT
            DISTINCT LOWER(TRIM(COALESCE(pim.style_id,ccv.colorVariantCode))) AS style
        FROM
            [rawOne11].[sap_ccv2] AS ccv
        LEFT JOIN
            rawone11.v_pim_articles_data AS pim
        ON
            pim.str_ean=ccv.ean
        WHERE
            stock > 0
    """
    with pyodbc.connect(CONNECTION_STRING) as con:
        df = pd.read_sql(SQL,con)
        
    if df.empty:
        logger.info('Table is empty!!!')
        
    return set(df['style'])

def get_orders(styles = None, from_date=None, to_date=None) -> pd.DataFrame:
    """
    Retrieve order data from database within a given date range.
    
    Parameters
    ----------
    from_date : datetime.date, optional
        Start date for filtering orders. Defaults to January 1, 2024 if not provided.
    to_date : datetime.date, optional
        End date for filtering orders. Defaults to today's date if not provided.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the retrieved orders with the following columns:
        - date (datetime): Order creation date.
        - quantity (int): Ordered quantity.
        - unit_price_vat_excl (float): Unit price without vat.
        - country_code (str): Country code of the buyer.
        - brand (str): Normalized item producer (brand).
        - product_name (str): Concatenated brand and matchcode description.
        - style (str): Generic SAP material number.
        - ean (str): EAN of the product
    """
    if not from_date:
        from_date = dt.date(2024,1,1)
    if not to_date:
        to_date = dt.date.today() 
        
    SQL = f"""
        SELECT
            hd.CreationDate AS date,
            o.OrderQuantity AS quantity,
            o.netAmount * ISNULL(RateToEUR, 1) / o.OrderQuantity AS unit_price_vat_excl,
            COALESCE(RIGHT(hd.SalesOffice, 2), 'NA') AS country_code,
            LOWER(TRIM(a.brand)) AS brand,
            LTRIM(RTRIM(LOWER(TRIM(a.brand)) + ' ' + REPLACE(LOWER(TRIM(a.name)), LOWER(TRIM(a.brand)), ''))) AS product_name,
            LOWER(TRIM(a.style_id)) AS style
        FROM [raw11ts].[sap_order_headers] hd
        INNER JOIN [raw11ts].[sap_orders] o 
            ON hd.SalesOrder = o.SalesOrder
        INNER JOIN [rawone11].[v_pim_articles_data] a
            ON a.str_ean = o.InternationalArticleNumber
        LEFT JOIN dbo.conversion_rates_daily cr  
            ON  cr.RateDate = hd.CreationDate 
            AND cr.currency = hd.PaymentCurrency
        WHERE 
            hd.SalesOrganization = '1300' -- Kickz
            AND hd.DistributionChannel = '10' -- E-commer
            AND a.name IS NOT NULL
            AND a.style_id IS NOT NULL
            AND a.brand IS NOT NULL
            AND hd.CreationDate >= '{from_date}'
            AND hd.CreationDate <= '{to_date}';
        """

    with pyodbc.connect(CONNECTION_STRING) as con:
        df = pd.read_sql(SQL,con)
    
    if df.empty:
        logger.info('Table is empty!!!')
        
    if styles:
        df = df[df['style'].isin(styles)]
        
    return df

def get_google_ads_data(from_date=None, to_date=dt.date.today()) -> pd.DataFrame:
    """
    Retrieve aggregated Google Ads performance data for Kickz campaigns.

    Parameters
    ----------
    from_date : datetime.date, optional
        Start date for filtering ads data. Defaults to 3 days before today if not provided.
    to_date : datetime.date, optional
        End date for filtering ads data. Defaults to today's date if not provided.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the aggregated Google Ads data with the following columns:
        - date (datetime): Reporting date.
        - country_code (str): Country code parsed from `account_name` (AT, DE, ..).
        - brand (str): Normalized producer/brand name from product metadata.
        - style (str): Generic SAP material number from product metadata.
        - impressions (int): Total ad impressions.
        - clicks (int): Total ad clicks.
        - cost (float): Total ad spend.
    """
    if not from_date:
        from_date = dt.date.today() - dt.timedelta(days=3)
    if not to_date:
        to_date = dt.date.today() 
        
    SQL = f"""
        SELECT
            g.date,
            UPPER(RIGHT(g.account_name, 2)) AS country_code,
            LOWER(TRIM(a.brand)) AS brand,
            LOWER(TRIM(a.style_id)) AS style,
            SUM(g.impressions) AS impressions,
            SUM(g.clicks) AS clicks,
            SUM(g.cost) AS cost
        FROM
            [one11].[googleAds_products] g
        INNER JOIN [rawone11].[v_pim_articles_data] a
            ON a.material_number = g.product_id
        WHERE
            account_name LIKE '%kickz%'
            AND g.date >= '{from_date}'
            AND g.date <= '{to_date}'
        GROUP BY
            date, 
            UPPER(RIGHT(account_name, 2)), 
            LOWER(TRIM(a.brand)), 
            LOWER(TRIM(a.style_id));
    """

    with pyodbc.connect(CONNECTION_STRING) as con:
        df = pd.read_sql(SQL,con)
    
    if df.empty:
        logger.info('Table is empty!!!')
        
    return df

def get_style_items_categories(styles = None, as_dict=False) -> pd.DataFrame: 
    """
    Retrieve product metadata (brand, product name, style, and category hierarchy) 
    for active Kickz items.

    Parameters
    ----------
    styles : list of str, optional
        List of style IDs to filter results. If None, all styles are returned.
    as_dict : bool, default False
        If True, return results as a dictionary keyed by style.
    """
    SQL = f"""
        SELECT
        DISTINCT
            LOWER(TRIM(a.brand)) AS brand,
            LTRIM(RTRIM(LOWER(TRIM(a.brand)) + ' ' + REPLACE(LOWER(TRIM(a.name)), LOWER(TRIM(a.brand)), ''))) AS product_name,
            LOWER(TRIM(a.style_id)) AS style,
            s.category as item_category,
            s.productDivision as item_group0,
            s.productSub as item_group1,
            s.productSubDivision as item_group2         
        FROM [product].[one11_styles] AS s
        INNER JOIN [rawone11].[v_pim_articles_data] AS a
        ON s.brand = a.brand
        AND s.style_id = a.style_id
        WHERE a.item_shop_active_kickz = 1
            AND a.name IS NOT NULL
            AND a.style_id IS NOT NULL
            AND a.brand IS NOT NULL
        """
        
    with pyodbc.connect(CONNECTION_STRING) as c:
        df = pd.read_sql(SQL, c)
        
    if styles:
        df = df[df['style'].isin(styles)]
        
    if df.empty:
        logger.info('Table is empty!!!')
    
    if as_dict:
        return df.drop_duplicates('style').set_index('style').to_dict(orient='index')
        
    return df 

def load_prices(styles=None):
    """
    Load product pricing data from the database.

    Executes a SQL query on the [rawKickz].[product_sales_price] table to 
    retrieve style, country code, currency, local sale price, and base price. 
    Filters out records with missing or zero values for `rrp` and `sale_price`. 

    If a list of styles is provided, the result is further filtered to include 
    only those styles.

    Parameters
    ----------
    styles : list[str] | None, optional
        List of style identifiers to filter the results. If None (default),
        all styles are included.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the following columns:
        - style
        - country_code
        - currency
        - price_local
        - base_price_local
    """
    SQL = """
        SELECT 
            LOWER(TRIM(style)) AS style,
            country AS country_code,
            UPPER(currency) AS currency,
            sale_price AS price_local,
            rrp AS base_price_local
        FROM [rawKickz].[product_sales_price]
        WHERE rrp IS NOT NULL
            AND rrp != 0
            AND sale_price IS NOT NULL
            AND sale_price != 0
    """
    with pyodbc.connect(CONNECTION_STRING) as c:
        df = pd.read_sql(SQL, c)
        
    if styles:
        df = df[df['style'].isin(styles)]
        
    if df.empty:
        logger.info('Table is empty!!!')
        
    return df

def get_prices_with_VAT(pricing_logic_data, as_dict=True):
    styles = pricing_logic_data['styles']
    conversion_rates = pricing_logic_data['conversion_rates']
    
    # processing
    df_conersion_rates = pd.DataFrame(conversion_rates, index=['conversion_rate'])\
                           .transpose()\
                           .reset_index()\
                           .rename(columns={'index': 'currency'})
    df_prices = load_prices(styles)
    df_prices = df_prices.merge(df_conersion_rates, on='currency')
    df_prices['price_EUR'] = df_prices['price_local'] / df_prices['conversion_rate']
    df_prices['base_price_EUR'] = df_prices['base_price_local'] / df_prices['conversion_rate']
    
    ########## TEMPORARY ########################
    df_material_number = load_material_number_mapper()
    df_manual_base_price = pd.read_excel(
        'AUTOMATIC PRICING BUCKETZ AND NEW ERA.xlsx',
        usecols=['material_number','UVP_KICKZ_EUR'],
    ).dropna()
    df_manual_base_price = df_manual_base_price[['material_number','UVP_KICKZ_EUR']].merge(df_material_number, on='material_number')
    df_prices = df_prices.merge(df_manual_base_price, on='style', how='left')
    df_prices.loc[df_prices['UVP_KICKZ_EUR'].notna(), 'base_price_EUR'] = df_prices.loc[df_prices['UVP_KICKZ_EUR'].notna(), 'UVP_KICKZ_EUR']
    ########## TEMPORARY ########################
    
    if as_dict:
        prices_dct = df_prices[['style','country_code','price_EUR','base_price_EUR','price_local']]\
                        .drop_duplicates(['style','country_code'])\
                        .set_index(['style','country_code'])\
                        .to_dict('index')
        return prices_dct

    return df_prices

def rcmnd_rule_increase(data):
    our_price = data['price']
    base_price = data['base_price']
    
    # rozhodujeme sa len podla stylovej konkurencie co ma na sklade 
    style_important_competitors_prices_stock = [data['style_important_competitors_prices'][index] 
                                                for index, value in enumerate(data['style_important_competitors_in_stock'])
                                                if value ==1]
    ########## PUSTIL SOM ZASE, ZE LEN PODLA SKLADU.
    # style_important_competitors_prices_stock = data['style_important_competitors_prices']
    ##########
    
    # ak mame konkurenciu cez styl
    ## NODE 1 ##
    if style_important_competitors_prices_stock:
        data['nodes_path'] += '1'
        
        style_competitors_price_min = np.min(style_important_competitors_prices_stock)
        
        # Konkurencia so stylom ma vacsie ceny, navysime aby sme boli o 2% nizsi ako najlacnejsia cena
        ## NODE 1 ##
        if base_price < style_competitors_price_min:
            data['nodes_path'] += '3'
            possible_price = max([base_price * 0.95, our_price])
            recom_price = min([possible_price, base_price])
            
        elif our_price < style_competitors_price_min:
            data['nodes_path'] += '1'
            possible_price = max([style_competitors_price_min * 0.98, our_price])
            recom_price = min([possible_price, base_price])
            
        # Konkurencia so stylom ma nizsie ceny, navysime o 1%
        ## NODE 2 ##
        elif our_price > style_competitors_price_min:
            data['nodes_path'] += '2'
            possible_price = our_price * 1.01
            recom_price = min([possible_price, base_price])
            
        ## NODE 3 ##
        else:
            data['nodes_path'] += '4'
            recom_price = min(base_price, our_price*1.05)
            
    # ak nemame konkurenciu cez styl
    ## NODE 2 ##
    else:
        data['nodes_path'] += '2'
        
        # rozhodujeme sa len podla produktovej konkurencie co ma na sklade 
        product_important_competitors_prices_stock = [data['product_important_competitors_prices'][index] 
                                                for index, value in enumerate(data['product_important_competitors_in_stock'])
                                                if value ==1]
        # ak mame konkurenciu cez produkt
        ## NODE 1 ##
        if product_important_competitors_prices_stock:
            data['nodes_path'] += '1'
            product_competitors_price_max = np.max(product_important_competitors_prices_stock)
            
            # navysime o 1%, ale aby sme neboli najdrahsi z celej konkurencie.
            ## NODE 1 ##
            if (our_price*1.01) < product_competitors_price_max:
                data['nodes_path'] += '1'
                possible_price = our_price*1.01
                recom_price = min([possible_price, base_price])
            
            # sme lacnejsi ako max konkurencia, zvysime tak aby sme ju o kusok podliezli
            ## NODE 2 ##
            elif our_price < product_competitors_price_max:
                data['nodes_path'] += '2'
                possible_price = max([product_competitors_price_max * 0.99, our_price])
                recom_price = min([possible_price, base_price])
                
            ## NODE 3 ##
            else:
                data['nodes_path'] += '3'
                recom_price = min([our_price * 1.01, base_price])
        
        # nemame ziadnu konkurenciu
        ## NODE 2 ##
        else:
            data['nodes_path'] += '2'
            possible_price = our_price * 1.02
            recom_price = min([possible_price, base_price])
    
    recom_price = max([recom_price, our_price*1.02])
    
    return recom_price


def rcmnd_rule_decrease(data, alone_on_market_sale = 0.98):
    our_price = data['price']
    our_min_possible_price = data['base_price'] * (1 - data['max_discount'])
    
#     if np.isnan(our_min_possible_price) or data['base_price'] == 0:
#         our_min_possible_price = data['price'] * (1 - data['max_discount'])
        
#     # rozhodujeme sa len podla stylovej konkurencie co ma na sklade 
#     style_important_competitors_prices_stock = [data['style_important_competitors_prices'][index] 
#                                                 for index, value in enumerate(data['style_important_competitors_in_stock'])
#                                                 if value ==1]
    ##########
    style_important_competitors_prices_stock = data['style_important_competitors_prices']
    ##########
    
    # ak mame konkurenciu cez styl
    ## NODE 1 ##
    if style_important_competitors_prices_stock:
        data['nodes_path'] += '1'
        style_competitors_price_min = np.min(style_important_competitors_prices_stock)
        style_competitors_price_max = np.max(style_important_competitors_prices_stock)
        
        
        if our_price < style_competitors_price_min:
            data['nodes_path'] += '4'
#             recom_price = np.max([our_min_possible_price,our_price * 0.99])
            # if we have competition with style and we are already the lowest, we stay on the same price
            recom_price = np.max([our_min_possible_price, our_price])
        
        # vsetky konkurencie so stylom maju mensiu cenu ako nas limit, klesneme o 10%.
        ## NODE 2 ##
        elif our_min_possible_price > style_competitors_price_max:
            data['nodes_path'] += '2'
            possible_prices = [our_price*0.9 if (our_price*0.9) > our_min_possible_price else our_min_possible_price]
            possible_prices.append(our_price)
            recom_price = np.min(possible_prices)
        
        # Podlezieme o percento konkurenciu avsak s ohladom na nasu minimalnu moznu cenu
        ## NODE 3 ##
        else:
            data['nodes_path'] += '3'
            possible_prices = [p*0.99 for p in style_important_competitors_prices_stock if (p*0.99 >= our_min_possible_price)]
            possible_prices.append(our_price)
            recom_price = np.min(possible_prices)
    
    # ak nemame konkurenciu cez styl
    ## NODE 2 ##
    else:
        data['nodes_path'] += '2'
        
        # rozhodujeme sa len podla produktovej konkurencie co ma na sklade 
        product_important_competitors_prices_stock = [data['product_important_competitors_prices'][index] 
                                                for index, value in enumerate(data['product_important_competitors_in_stock'])
                                                if value ==1]
        # ak mame konkurenciu cez produkt
        ## NODE 1 ##
        if product_important_competitors_prices_stock:
            data['nodes_path'] += '1'
            # ak nie sme sami na trhu tak znizime o 2% ak mozme
            possible_prices = [our_price*0.98 if (our_price*0.98) > our_min_possible_price else our_min_possible_price]
            possible_prices.append(our_price)
            recom_price = np.min(possible_prices)
        
        ## NODE 2 ##
        else:
            data['nodes_path'] += '2'
            # sme sami na trhu, ak mozme znizime o alone_on_market_sale %
            possible_prices = [our_price*alone_on_market_sale if (our_price*alone_on_market_sale) > our_min_possible_price 
                               else our_min_possible_price]
            possible_prices.append(our_price)
            recom_price = np.min(possible_prices)
    
    return recom_price


def sell_power_tree(data, allow_increase):
    # minimalna mozna cena
    our_min_possible_price = data['base_price'] * (1 - data['max_discount'])
    our_max_possible_price = data['base_price'] * (1 - data['min_discount'])

    # ak sme zmenili cenu v poslednych 7 dnoch a konkurencia cez style neexistuje 
    # alebo sme zmenili cenu v poslednych 7 dnoch a konkurencia existuje tak pozerame ci zmenili ceny
    ## NODE 5 ##
    if ((data['changed_last_days'] and not data['style_important_competitors_price_change_day'])\
    or  (data['changed_last_days'] and data['style_important_competitors_price_change_day']\
         and np.min(data['style_important_competitors_price_change_day']) == 0
         and np.max(data['style_important_competitors_price_change_day']) == 0)):

        data['nodes_path'] += '5'
        recom_change = 'CHANGED LAST DAYS'
        recom_price = data['price']

        ## NODE 1 ##
    elif (np.isnan(data['price'])
       or np.isnan(data['sell_power_week'])
       or np.isnan(data['sell_power_day'])
       or np.isnan(data['last_day_sell_power_week'])
       or data['is_new_product']):
            
        data['nodes_path'] += '1'
        recom_change = 'NOT ENOUGH DATA'
        recom_price = data['price']
        
    ## NODE 4 ##
    elif data['sell_power_day'] >= 13 and data['sell_power_day'] <= 15:
        data['nodes_path'] += '4'
        recom_price = data['price']
        recom_change = 'KEEP'

    ## NODE 2 ##
    elif (data['sell_power_week'] <= data['last_day_sell_power_week']) or (data['sell_power_week'] < 20):
        data['nodes_path'] += '2'
        recom_change = 'DECREASE'
        recom_price = rcmnd_rule_decrease(data)

    ## NODE 3 ##
    elif data['sell_power_week'] > data['last_day_sell_power_week'] and allow_increase:
        data['nodes_path'] += '3'
        recom_change = 'INCREASE'
        recom_price = rcmnd_rule_increase(data)

    else:
        data['nodes_path'] += '??'
        recom_change = 'KEEP'
        recom_price = data['price']
        
    
    # ak je zlava vacsia ako 0.1% a mensia ako 5% tak zlava je 5%
    if (recom_price < data['base_price']*0.999) and (recom_price > data['base_price']*0.95):
        recom_price = 0.95 * data['base_price']
                          
    recom_price = max(recom_price, our_min_possible_price)
    recom_price = min(recom_price, our_max_possible_price)
            
    
    data['recom_change'] = recom_change
    data['recom_price'] = recom_price
        
    return data


def margin_tree(data):
    # minimalna mozna cena
    our_min_possible_price = data['base_price'] * (1 - data['max_discount'])
    our_max_possible_price = data['base_price'] * (1 - data['min_discount'])

    # ak sme zmenili cenu v poslednych 7 dnoch a konkurencia cez style neexistuje 
    # alebo sme zmenili cenu v poslednych 7 dnoch a konkurencia existuje tak pozerame ci zmenili ceny
    ## NODE 5 ##
    if ((data['changed_last_days'] and not data['style_important_competitors_price_change_day'])\
    or  (data['changed_last_days'] and data['style_important_competitors_price_change_day']\
         and np.min(data['style_important_competitors_price_change_day']) == 0
         and np.max(data['style_important_competitors_price_change_day']) == 0)):

        data['nodes_path'] += '5'
        recom_change = 'CHANGED LAST DAYS'
        recom_price = data['price']

        ## NODE 1 ##
    elif (np.isnan(data['price'])
     or data['is_new_product']):
        data['nodes_path'] += '1'
        recom_change = 'NOT ENOUGH DATA'
        recom_price = data['price']
        
    ## NODE 2 ##
    elif data['diff_to_expected_margin'] > 2: # ak sme viac ako 2% nad ocakavannou marzou
        data['nodes_path'] += '2'
        recom_change = 'DECREASE'
        recom_price = rcmnd_rule_decrease(data)

    ## NODE 3 ##
    elif data['diff_to_expected_margin'] < -2: # ak sme viac ako 2% pod ocakavannou marzou
        data['nodes_path'] += '3'
        recom_change = 'INCREASE'
        recom_price = rcmnd_rule_increase(data)
                  
    ## NODE 4 ##
    else:
        data['nodes_path'] += '4'
        recom_price = data['price']
        recom_change = 'KEEP'
    
    # ak je zlava vacsia ako 0.1% a mensia ako 5% tak zlava je 5%
    if (recom_price < data['base_price']*0.999) and (recom_price > data['base_price']*0.95):
        recom_price = 0.95 * data['base_price']
                          
    recom_price = max(recom_price, our_min_possible_price)
    recom_price = min(recom_price, our_max_possible_price)
            
    
    data['recom_change'] = recom_change
    data['recom_price'] = recom_price
        
    return data


def total_demand_tree(data):
    # minimalna mozna cena
    our_min_possible_price = data['base_price'] * (1 - data['max_discount'])
    our_max_possible_price = data['base_price'] * (1 - data['min_discount'])


    # ak sme zmenili cenu v poslednych 7 dnoch a konkurencia cez style neexistuje 
    # alebo sme zmenili cenu v poslednych 7 dnoch a konkurencia existuje tak pozerame ci zmenili ceny
    ## NODE 5 ##
    if ((data['changed_last_days'] and not data['style_important_competitors_price_change_day'])\
    or  (data['changed_last_days'] and data['style_important_competitors_price_change_day']\
         and np.min(data['style_important_competitors_price_change_day']) == 0
         and np.max(data['style_important_competitors_price_change_day']) == 0)):

        data['nodes_path'] += '5'
        recom_change = 'CHANGED LAST DAYS'
        recom_price = data['price']

        ## NODE 1 ##
    elif (np.isnan(data['price'])
      or data['is_new_product']):
        data['nodes_path'] += '1'
        recom_change = 'NOT ENOUGH DATA'
        recom_price = data['price']
        
    ## NODE 2 ##
    elif (data['total_demand'] < 0.75) or ((data['total_demand'] < 1) and (data['sold_items_7_days'] < 8)):
        data['nodes_path'] += '2'
        recom_change = 'DECREASE'
        recom_price = rcmnd_rule_decrease(data)

    ## NODE 3 ##
    elif data['total_demand'] > 1:
        data['nodes_path'] += '3'
        recom_change = 'INCREASE'
        recom_price = rcmnd_rule_increase(data)
                  
    ## NODE 4 ##
    else:
        data['nodes_path'] += '4'
        recom_price = data['price']
        recom_change = 'KEEP'
        
    # ak je zlava vacsia ako 0.1% a mensia ako 5% tak zlava je 5%
    if (recom_price < data['base_price']*0.999) and (recom_price > data['base_price']*0.95):
        recom_price = 0.95 * data['base_price']
                          
    recom_price = max(recom_price, our_min_possible_price)
    recom_price = min(recom_price, our_max_possible_price)
        
    data['recom_change'] = recom_change
    data['recom_price'] = recom_price
        
    return data

def sale_tree(data, allow_increase=True, alone_on_market_sale=0.95):
    # minimalna mozna cena
    our_min_possible_price = data['base_price'] * (1 - data['max_discount'])
    our_max_possible_price = data['base_price'] * (1 - data['min_discount'])


    # ak sme zmenili cenu v poslednych 7 dnoch a konkurencia cez style neexistuje 
    # alebo sme zmenili cenu v poslednych 7 dnoch a konkurencia existuje tak pozerame ci zmenili ceny
    ## NODE 5 ##
    if ((data['changed_last_days'] and not data['style_important_competitors_price_change_day'])\
    or  (data['changed_last_days'] and data['style_important_competitors_price_change_day']\
         and np.min(data['style_important_competitors_price_change_day']) == 0
         and np.max(data['style_important_competitors_price_change_day']) == 0)):

        data['nodes_path'] += '5'
        recom_change = 'CHANGED LAST DAYS'
        recom_price = data['price']

        ## NODE 1 ##
    elif (np.isnan(data['price'])
      or data['is_new_product']):         
        data['nodes_path'] += '1'
        recom_change = 'NOT ENOUGH DATA'
        recom_price = data['price']
        
    ## NODE 2 ##
    elif ((data['total_demand'] < 0.8) or 
         ((data['total_demand'] < 1) and (data['sold_items_7_days'] < 8))):
        data['nodes_path'] += '2'
        recom_change = 'DECREASE'
        recom_price = rcmnd_rule_decrease(data, alone_on_market_sale)

    ## NODE 3 ##
    elif data['total_demand'] > 1 and allow_increase:
        data['nodes_path'] += '3'
        recom_change = 'INCREASE'
        recom_price = rcmnd_rule_increase(data)
                  
    ## NODE 4 ##
    else:
        data['nodes_path'] += '4'
        recom_price = data['price']
        recom_change = 'KEEP'
        
    # ak je zlava vacsia ako 0.1% a mensia ako 5% tak zlava je 5%
    if (recom_price < data['base_price']*0.999) and (recom_price > data['base_price']*0.95):
        recom_price = 0.95 * data['base_price']
                          
    recom_price = max(recom_price, our_min_possible_price)
    recom_price = min(recom_price, our_max_possible_price)
        
    data['recom_change'] = recom_change
    data['recom_price'] = recom_price

    return data

def keep_tree(data):    
    data['recom_change'] = 'KEEP'
    data['recom_price'] = data['price']
        
    return data

def increase_tree(data):
    # minimalna mozna cena
    our_min_possible_price = data['base_price'] * (1 - data['max_discount'])
    our_max_possible_price = data['base_price'] * (1 - data['min_discount'])


    # ak sme zmenili cenu v poslednych 7 dnoch a konkurencia cez style neexistuje 
    # alebo sme zmenili cenu v poslednych 7 dnoch a konkurencia existuje tak pozerame ci zmenili ceny
    ## NODE 5 ##
    if ((data['changed_last_days'] and not data['style_important_competitors_price_change_day'])\
    or  (data['changed_last_days'] and data['style_important_competitors_price_change_day']\
         and np.min(data['style_important_competitors_price_change_day']) == 0
         and np.max(data['style_important_competitors_price_change_day']) == 0)):

        data['nodes_path'] += '5'
        recom_change = 'CHANGED LAST DAYS'
        recom_price = data['price']

        ## NODE 1 ##
    elif (np.isnan(data['price'])
      or data['is_new_product']):
        data['nodes_path'] += '1'
        recom_change = 'NOT ENOUGH DATA'
        recom_price = data['price']

    ## NODE 2 ##
    else:
        data['nodes_path'] += '2'
        recom_change = 'INCREASE'
        recom_price = rcmnd_rule_increase(data)
        
    # ak je zlava vacsia ako 0.1% a mensia ako 5% tak zlava je 5%
    if (recom_price < data['base_price']*0.999) and (recom_price > data['base_price']*0.95):
        recom_price = 0.95 * data['base_price']
                          
    recom_price = max(recom_price, our_min_possible_price)
    recom_price = min(recom_price, our_max_possible_price)
        
    data['recom_change'] = recom_change
    data['recom_price'] = recom_price
        
    return data

def decrease_tree(data):
    # minimalna mozna cena
    our_min_possible_price = data['base_price'] * (1 - data['max_discount'])
    our_max_possible_price = data['base_price'] * (1 - data['min_discount'])

    # ak sme zmenili cenu v poslednych 7 dnoch a konkurencia cez style neexistuje 
    # alebo sme zmenili cenu v poslednych 7 dnoch a konkurencia existuje tak pozerame ci zmenili ceny
    ## NODE 5 ##
    if ((data['changed_last_days'] and not data['style_important_competitors_price_change_day'])\
    or  (data['changed_last_days'] and data['style_important_competitors_price_change_day']\
         and np.min(data['style_important_competitors_price_change_day']) == 0
         and np.max(data['style_important_competitors_price_change_day']) == 0)):

        data['nodes_path'] += '5'
        recom_change = 'CHANGED LAST DAYS'
        recom_price = data['price']

        ## NODE 1 ##
    elif (np.isnan(data['price']) or data['is_new_product']):
        data['nodes_path'] += '1'
        recom_change = 'NOT ENOUGH DATA'
        recom_price = data['price']
        
    else:
        data['nodes_path'] += '2'
        recom_change = 'DECREASE'
        recom_price = rcmnd_rule_decrease(data)
    
    # ak je zlava vacsia ako 0.1% a mensia ako 5% tak zlava je 5%
    if (recom_price < data['base_price']*0.999) and (recom_price > data['base_price']*0.95):
        recom_price = 0.95 * data['base_price']
                          
    recom_price = max(recom_price, our_min_possible_price)
    recom_price = min(recom_price, our_max_possible_price)
        
    data['recom_change'] = recom_change
    data['recom_price'] = recom_price
        
    return data

def destroy_competitors_tree(data):
    our_min_possible_price = data['base_price'] * (1 - data['max_discount'])
    our_max_possible_price = data['base_price'] * (1 - data['min_discount'])
    
    if (np.isnan(data['price'])
      or data['is_new_product']):
        data['nodes_path'] += '1'
        recom_change = 'NOT ENOUGH DATA'
        recom_price = data['price']
    
    else:
        data['nodes_path'] += '2'
        
        our_price = data['price']
        style_important_competitors_prices = data['style_important_competitors_prices']
        
        # ak mame konkurenciu podlezieme ju o 2% ak mozeme
        if style_important_competitors_prices:
            data['nodes_path'] += '1'
            recom_change = 'DECREASE'
            
            possible_prices = [p*0.98 for p in style_important_competitors_prices if (p*0.98 >= our_min_possible_price)]
            if possible_prices: # ak mame konkurenciu ktoru mozme este podliezt 
                recom_price = np.min(possible_prices)
            else:
                recom_price = our_min_possible_price
        
        else:
            data['nodes_path'] += '2'
            recom_change = 'KEEP'
            recom_price = our_price 

    recom_price = max(recom_price, our_min_possible_price)
    recom_price = min(recom_price, our_max_possible_price)
        
    data['recom_change'] = recom_change
    data['recom_price'] = recom_price
    
    return data

def independent_scoring_tree(data):    
    if data['category'] == 'DESTROY_COMPETITORS':
        return destroy_competitors_tree(data)
    
    elif data['category'] in [
        'IMP','TEAM_SALE','CARRYOVERS','DROPSHIPMENT',
        'TEAMSPORT_OVERSTOCK', 'TOTAL_CLEARANCE','INDOOR_SHOES'
    ]:
        if not np.isnan(data['diff_to_expected_margin']) and data['expected_margin_use_in_country']:
            return margin_tree(data)
        return total_demand_tree(data)
    
    elif data['category'] in ['HARD_SALE','SOFT_SALE', 'ENTRY_SALE']:
        return sell_power_tree(data, allow_increase=True)
    
    else:
        return sell_power_tree(data, allow_increase=True)

def tree(data):
    logger.debug(f'scoring {data["style"]}')
    
    # scoring acccording to total_demand per group
    if data['group_logic'] == 'AUTO':
        return total_demand_tree(data)
    # group increase
    elif data['group_logic'] == 'INCREASE':
        return increase_tree(data)
    # group decrease
    elif data['group_logic'] == 'DECREASE':
        return decrease_tree(data)
    # group keep
    elif data['group_logic'] == 'KEEP':
        return keep_tree(data)
    # scoring without respect to groups
    else:
        return independent_scoring_tree(data)
     

def find_optimal_prices(pricing_logic_data):
    df_results = pd.DataFrame(pricing_logic_data['data_for_pricing'])
    df_results.to_csv('data_for_pricing_last_run.csv',index=False)
    
    df_final = df_results.apply(tree, axis=1)
    df_final['date'] = pricing_logic_data['run_time'].strftime('%Y-%m-%d %H:%M:%S')
    
    # if we did not change price
    df_final.loc[~df_final['recom_change'].isin(['DECREASE','INCREASE']), 'last_changed_days_ago'] = df_final.loc[~df_final['recom_change'].isin(['DECREASE','INCREASE']), 'last_changed_days_ago'] + 1
    # if we changed price last run
    df_final.loc[df_final['recom_change'].isin(['DECREASE','INCREASE']), 'last_changed_days_ago'] = 0
    

    return df_final

class S3ProductsToScore:
    BUCKET_NAME = 'autopricing'
    CLIENT = 'kickz'
    FOLDER = 'products_to_score'
    DATE_FORMAT = '%Y%m%d%H%M%S'
                
    @staticmethod
    def store(df):
        timestamp = dt.datetime.now().strftime(S3ProductsToScore.DATE_FORMAT)
        
        # convert to str
        cols = df.select_dtypes('object').columns.tolist()
        df[cols] = df[cols].astype(str)
        
        
        full_path = f's3://{S3ProductsToScore.BUCKET_NAME}/{S3ProductsToScore.CLIENT}/{S3ProductsToScore.FOLDER}/{timestamp}.parquet'
        logger.info(f"Storing: {full_path}...")
            
        df.to_parquet(path=full_path, compression='gzip', index=False)
            
            
    @staticmethod        
    def load_latest(columns=None, query=None, **kwargs):     
        file_names = sorted(
            [
                filename for filename in S3.get_all_objects_from_bucket(
                    bucket_name = S3ProductsToScore.BUCKET_NAME, 
                    prefix = f'{S3ProductsToScore.CLIENT}/{S3ProductsToScore.FOLDER}', 
                    only_keys=True
                )
                if filename.endswith('.parquet')
            ]
        )
        
        # latest one
        full_path = f's3://{S3ProductsToScore.BUCKET_NAME}/{file_names[-1]}'
        logger.info(f"Loading: {full_path}...")
        
        df = pd.read_parquet(
            full_path, 
            columns = columns, 
            **kwargs
            )
        
        if query is not None:
            df = df.query(query)
        
        return df.replace('None', np.nan)
    
class S3RcmndHistory:
    BUCKET_NAME = 'autopricing'
    CLIENT = 'kickz'
    FOLDER = 'rcmnd_history'
    DATE_FORMAT = '%Y%m%d'
    
    @staticmethod
    def store_as_json(df):
        df['date'] = pd.to_datetime(df['date'])
        
        for date in df['date'].unique():
            df_date = df[df['date'] == date]
            
            filename = df_date['date'].dt.date.unique()[0].strftime(S3RcmndHistory.DATE_FORMAT)
            S3.store_file_in_bucket(bucket_name = S3RcmndHistory.BUCKET_NAME, 
                                    file_name = f'{S3RcmndHistory.CLIENT}/{S3RcmndHistory.FOLDER}/{filename}.json', 
                                    file = df_date.to_json())
                
    @staticmethod
    def store(df):
        df['date'] = pd.to_datetime(df['date'])
        
        # convert to str
        cols = df.select_dtypes('object').columns.tolist()
        df[cols] = df[cols].astype(str)
        
        for date in df['date'].unique():
            df_date = df[df['date'] == date]
            
            filename = df_date['date'].dt.date.unique()[0].strftime(S3RcmndHistory.DATE_FORMAT)
            full_path = f's3://{S3RcmndHistory.BUCKET_NAME}/{S3RcmndHistory.CLIENT}/{S3RcmndHistory.FOLDER}/{filename}.parquet'
            
            df_date.to_parquet(path=full_path, compression='gzip', index=False)
            
    @staticmethod        
    def load_as_json(from_date, to_date, columns=None, query=None):
        file_names = [
            filename for filename in S3.get_all_objects_from_bucket(bucket_name = S3RcmndHistory.BUCKET_NAME, 
                                                                    prefix = f'{S3RcmndHistory.CLIENT}/{S3RcmndHistory.FOLDER}', 
                                                                    only_keys=True)
            if filename.endswith('.json')
        ]
        
        dates_to_download = [(from_date + dt.timedelta(days=x)).strftime(S3RcmndHistory.DATE_FORMAT) for x in range((to_date - from_date).days + 1)]
        
        dataframes = []
        for file_name in file_names:
            file_date = file_name[file_name.rfind('/')+1:file_name.rfind('.')]

            if file_date in dates_to_download:
                logger.info(f"Rcmnd history loading {file_name}...")
                json = S3.get_file_from_bucket(bucket_name= S3RcmndHistory.BUCKET_NAME, file_name=file_name)
                df_date = pd.read_json(json)
                
                if query is not None:
                    df_date = df_date.query(query)
                
                if columns is not None:
                    cols_to_take = set(df_date.columns).intersection(columns)
                    df_date = df_date[cols_to_take]

                dataframes.append(df_date)

        if not dataframes:
            raise Exception(f'Rcmnd history data not found between {from_date} and {to_date}')


        return pd.concat(dataframes, ignore_index=True)
            
    @staticmethod        
    def load(from_date, to_date, columns=None, query=None, literal_eval_cols=None, **kwargs):
        if literal_eval_cols is None:
            literal_eval_cols = []
            
        file_names = [
            filename for filename in S3.get_all_objects_from_bucket(bucket_name = S3RcmndHistory.BUCKET_NAME, 
                                                                    prefix = f'{S3RcmndHistory.CLIENT}/{S3RcmndHistory.FOLDER}', 
                                                                    only_keys=True)
            if filename.endswith('.parquet')
        ]
        
        dates_to_download = [(from_date + dt.timedelta(days=x)).strftime(S3RcmndHistory.DATE_FORMAT) for x in range((to_date - from_date).days + 1)]
        
        dataframes = []
        for file_name in file_names:
            file_date = file_name[file_name.rfind('/')+1:file_name.rfind('.')]

            if file_date in dates_to_download:
                full_path = f's3://{S3RcmndHistory.BUCKET_NAME}/{file_name}'
                logger.info(f"Rcmnd history loading {full_path}...")
                
                df_date = pd.read_parquet(full_path, columns=columns, **kwargs)
                
                if query is not None:
                    df_date = df_date.query(query)
                    
                for col in literal_eval_cols:
                    if col in df_date.columns:
                        df_date[col] = df_date[col].apply(lambda v: safe_literal_eval(v))
                        
                dataframes.append(df_date)

        if not dataframes:
            raise Exception(f'Rcmnd history data not found between {from_date} and {to_date}')

        return pd.concat(dataframes, ignore_index=True)
    
class S3RcmndHistory:
    BUCKET_NAME = 'autopricing'
    CLIENT = 'kickz'
    FOLDER = 'rcmnd_history'
    DATE_FORMAT = '%Y%m%d'
    
    @staticmethod
    def store_as_json(df):
        df['date'] = pd.to_datetime(df['date'])
        
        for date in df['date'].unique():
            df_date = df[df['date'] == date]
            
            filename = df_date['date'].dt.date.unique()[0].strftime(S3RcmndHistory.DATE_FORMAT)
            S3.store_file_in_bucket(bucket_name = S3RcmndHistory.BUCKET_NAME, 
                                    file_name = f'{S3RcmndHistory.CLIENT}/{S3RcmndHistory.FOLDER}/{filename}.json', 
                                    file = df_date.to_json())
                
    @staticmethod
    def store(df):
        df['date'] = pd.to_datetime(df['date'])
        
        # convert to str
        cols = df.select_dtypes('object').columns.tolist()
        df[cols] = df[cols].astype(str)
        
        for date in df['date'].unique():
            df_date = df[df['date'] == date]
            
            filename = df_date['date'].dt.date.unique()[0].strftime(S3RcmndHistory.DATE_FORMAT)
            full_path = f's3://{S3RcmndHistory.BUCKET_NAME}/{S3RcmndHistory.CLIENT}/{S3RcmndHistory.FOLDER}/{filename}.parquet'
            
            df_date.to_parquet(path=full_path, compression='gzip', index=False)
            
    @staticmethod        
    def load_as_json(from_date, to_date, columns=None, query=None):
        file_names = [
            filename for filename in S3.get_all_objects_from_bucket(bucket_name = S3RcmndHistory.BUCKET_NAME, 
                                                                    prefix = f'{S3RcmndHistory.CLIENT}/{S3RcmndHistory.FOLDER}', 
                                                                    only_keys=True)
            if filename.endswith('.json')
        ]
        
        dates_to_download = [(from_date + dt.timedelta(days=x)).strftime(S3RcmndHistory.DATE_FORMAT) for x in range((to_date - from_date).days + 1)]
        
        dataframes = []
        for file_name in file_names:
            file_date = file_name[file_name.rfind('/')+1:file_name.rfind('.')]

            if file_date in dates_to_download:
                logger.info(f"Rcmnd history loading {file_name}...")
                json = S3.get_file_from_bucket(bucket_name= S3RcmndHistory.BUCKET_NAME, file_name=file_name)
                df_date = pd.read_json(json)
                
                if query is not None:
                    df_date = df_date.query(query)
                
                if columns is not None:
                    cols_to_take = set(df_date.columns).intersection(columns)
                    df_date = df_date[cols_to_take]

                dataframes.append(df_date)

        if not dataframes:
            raise Exception(f'Rcmnd history data not found between {from_date} and {to_date}')


        return pd.concat(dataframes, ignore_index=True)
            
    @staticmethod        
    def load(from_date, to_date, columns=None, query=None, literal_eval_cols=None, **kwargs):
        if literal_eval_cols is None:
            literal_eval_cols = []
            
        file_names = [
            filename for filename in S3.get_all_objects_from_bucket(bucket_name = S3RcmndHistory.BUCKET_NAME, 
                                                                    prefix = f'{S3RcmndHistory.CLIENT}/{S3RcmndHistory.FOLDER}', 
                                                                    only_keys=True)
            if filename.endswith('.parquet')
        ]
        
        dates_to_download = [(from_date + dt.timedelta(days=x)).strftime(S3RcmndHistory.DATE_FORMAT) for x in range((to_date - from_date).days + 1)]
        
        dataframes = []
        for file_name in file_names:
            file_date = file_name[file_name.rfind('/')+1:file_name.rfind('.')]

            if file_date in dates_to_download:
                full_path = f's3://{S3RcmndHistory.BUCKET_NAME}/{file_name}'
                logger.info(f"Rcmnd history loading {full_path}...")
                
                df_date = pd.read_parquet(full_path, columns=columns, **kwargs)
                
                if query is not None:
                    df_date = df_date.query(query)
                    
                for col in literal_eval_cols:
                    if col in df_date.columns:
                        df_date[col] = df_date[col].apply(lambda v: safe_literal_eval(v))
                        
                dataframes.append(df_date)

        if not dataframes:
            raise Exception(f'Rcmnd history data not found between {from_date} and {to_date}')

        return pd.concat(dataframes, ignore_index=True)
