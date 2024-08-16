import os
import pandas as pd

def load_data(root_path: str) -> tuple:
    """
    Loads multiple CSV files from a specified directory and returns them as a tuple of DataFrames.

    Args:
    root_path (str): The directory path where the CSV files are stored.

    Returns:
    tuple: A tuple containing the following DataFrames:
        - customers: Loaded from 'Customers.csv'.
        - exchange_rates: Loaded from 'Exchange_Rates.csv'.
        - products: Loaded from 'Products.csv'.
        - sales: Loaded from 'Sales.csv'.
        - stores: Loaded from 'Stores.csv'.
    """
    customers = pd.read_csv(os.path.join(root_path, 'Customers.csv'), encoding='unicode_escape')
    exchange_rates = pd.read_csv(os.path.join(root_path, 'Exchange_Rates.csv'))
    products = pd.read_csv(os.path.join(root_path, 'Products.csv'))
    sales = pd.read_csv(os.path.join(root_path, 'Sales.csv'))
    stores = pd.read_csv(os.path.join(root_path, 'Stores.csv'))
    return customers, exchange_rates, products, sales, stores


def preprocess_customer_table(customer_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the customer data by standardizing column names, handling missing values, and formatting dates.

    Args:
    customer_df (pd.DataFrame): A DataFrame containing customer data.

    Returns:
    pd.DataFrame: A preprocessed DataFrame where:
        - Column names are standardized.
        - Missing 'state_code' values are imputed when 'state' is 'Napoli'.
        - The 'birthday' column is formatted to SQL date format.
    """
    def replace_nan_with_na(row):
        if row['state'] == 'Napoli' and pd.isna(row['state_code']):
            row['state_code'] = 'NA'
        return row

    customer_df = standardize_col_names(customer_df)
    customer_df = customer_df.apply(replace_nan_with_na, axis=1)
    customer_df = customer_df.apply(lambda row: format_date_column(row, date_col_name='birthday'), axis=1)
    return customer_df


def preprocess_product_table(product_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the product data by standardizing column names, creating a category ID, 
    and converting price columns to numeric values.

    Args:
    product_df (pd.DataFrame): A DataFrame containing product data.

    Returns:
    pd.DataFrame: A preprocessed DataFrame where:
        - Column names are standardized.
        - A 'category_id' is created based on 'Category' and 'Subcategory'.
        - The 'unit_cost_usd' and 'unit_price_usd' columns are converted to numeric values.
    """
    def create_product_id(row):
        index = product_category[(product_category['Subcategory'] == row['Subcategory']) & 
                                 (product_category['Category'] == row['Category'])]['index']
        return index.item()

    product_category = product_df.iloc[:, [-3, -1]].drop_duplicates().reset_index(drop=True).reset_index()
    product_df['category_id'] = [create_product_id(row[1]) for row in product_df.iterrows()]
    product_df = product_df.drop(['Subcategory', 'CategoryKey', 'Category', 'SubcategoryKey'], axis=1)

    product_df = standardize_col_names(product_df)
    product_category = standardize_col_names(product_category)
    product_df['unit_cost_usd'] = product_df['unit_cost_usd'].str.replace('$', '').str.replace(',', '').str.strip().astype(float)
    product_df['unit_price_usd'] = product_df['unit_price_usd'].str.replace('$', '').str.replace(',', '').str.strip().astype(float)
    return product_df


def preprocess_sale_table(sale_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the sales data by standardizing column names and formatting date columns.

    Args:
    sale_df (pd.DataFrame): A DataFrame containing sales data.

    Returns:
    pd.DataFrame: A preprocessed DataFrame where:
        - Column names are standardized.
        - The 'order_date' and 'delivery_date' columns are formatted to SQL date format.
        - The 'delivery_date' column is set to None if it is missing.
    """
    sale_df = standardize_col_names(sale_df)
    sale_df = sale_df.apply(lambda row: format_date_column(row, date_col_name='order_date'), axis=1)
    sale_df = sale_df.apply(lambda row: format_date_column(row, date_col_name='delivery_date'), axis=1)
    sale_df['delivery_date'] = sale_df['delivery_date'].apply(lambda x: None if pd.isna(x) else x)
    return sale_df


def preprocess_store_table(store_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the store data by standardizing column names, formatting date columns, 
    and filling missing values for square meters.

    Args:
    store_df (pd.DataFrame): A DataFrame containing store data.

    Returns:
    pd.DataFrame: A preprocessed DataFrame where:
        - Column names are standardized.
        - The 'open_date' column is formatted to SQL date format.
        - The 'square_meters' column is filled with 0 where values are missing.
    """
    store_df = standardize_col_names(store_df)
    store_df = store_df.apply(lambda row: format_date_column(row, date_col_name='open_date'), axis=1)
    store_df['square_meters'] = store_df['square_meters'].fillna(0)
    return store_df


def preprocess_exchange_rates_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the exchange rates data by standardizing column names and formatting the date column.

    Args:
    df (pd.DataFrame): A DataFrame containing exchange rates data.

    Returns:
    pd.DataFrame: A preprocessed DataFrame where:
        - Column names are standardized.
        - The 'date' column is formatted to SQL date format.
    """
    df = standardize_col_names(df)
    df = df.apply(lambda row: format_date_column(row, date_col_name='date'), axis=1)
    return df


# Utility functions

def standardize_col_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes the column names of a DataFrame by converting them to lowercase, 
    replacing spaces with underscores, and stripping any leading/trailing spaces.

    Args:
    df (pd.DataFrame): A DataFrame whose column names are to be standardized.

    Returns:
    pd.DataFrame: The DataFrame with standardized column names.
    """
    col_names = {val: val.lower().replace(' ', '_').strip() for val in df.columns}
    df.rename(columns=col_names, inplace=True)
    return df


def format_date_column(row: pd.Series, date_col_name: str) -> pd.Series:
    """
    Formats a date column in a DataFrame row to SQL date format (YYYY-MM-DD).

    Args:
    row (pd.Series): A row of data from the DataFrame.
    date_col_name (str): The name of the date column to be formatted.

    Returns:
    pd.Series: The DataFrame row with the formatted date column.
    """
    if not pd.isna(row[date_col_name]):
        date = row[date_col_name].split('/')
        row[date_col_name] = f"{date[-1]}-{date[0]}-{date[1]}"
    return row
