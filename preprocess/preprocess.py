import pandas as pd
import os

def load_data(root_path:str)->tuple:
    customers = pd.read_csv(os.path.join(root_path,'Customers.csv'), encoding='unicode_escape')
    # data_dictionary = pd.read_csv(os.path.join(root_path,'Data_Dictionary.csv'))
    exchange_rates = pd.read_csv(os.path.join(root_path,'Exchange_Rates.csv'))
    products = pd.read_csv(os.path.join(root_path,'Products.csv'))
    sales =  pd.read_csv(os.path.join(root_path,'Sales.csv'))
    stores = pd.read_csv(os.path.join(root_path,'Stores.csv'))
    return customers,exchange_rates,products,sales,stores




def preprocess_customer_table(customer_df: pd.DataFrame)->pd.DataFrame:
    #standardizing column_names

    def replace_nan_with_na(row):
        if row['state'] == 'Napoli' and pd.isna(row['state_code']):
            row['state_code'] = 'NA'
        return row
            
    #standardizing column names
    customer_df = standardize_col_names(customer_df)
    #imputing null values
    customer_df = customer_df.apply(replace_nan_with_na, axis=1)
    #formating birthday in sql 
    customer_df= customer_df.apply(lambda row : format_date_column(row,date_col_name='birthday'),axis = 1)
    return customer_df



def preprocess_product_table(product_df:pd.DataFrame)->pd.DataFrame:

        def create_product_id(row):
            index = product_category[(product_category['Subcategory'] == row['Subcategory']) & (product_category['Category'] == row['Category'])]['index']
            return index.item()

        product_category = product_df.iloc[:,[-3,-1]].drop_duplicates().reset_index(drop=True).reset_index()

        #extracting index from product_category table
        product_df['category_id'] = [create_product_id(row[1]) for row in product_df.iterrows()]
        product_df = product_df.drop(['Subcategory','CategoryKey','Category','SubcategoryKey'],axis=1)

        #standardizing col names
        product_df = standardize_col_names(product_df)
        product_category = standardize_col_names(product_category)

        #removing $ symbol
        product_df['unit_cost_usd'] = product_df['unit_cost_usd'].str.replace('$', '').str.replace(',','').str.strip().astype(float)
        product_df['unit_price_usd'] = product_df['unit_price_usd'].str.replace('$', '').str.replace(',','').str.strip().astype(float)
        

def preprocess_sale_table(sale_df: pd.DataFrame)-> pd.DataFrame:
        
        sale_df = standardize_col_names(sale_df)

        #formatting date
        sale_df= sale_df.apply(lambda row : format_date_column(row,date_col_name='order_date'),axis = 1)
        sale_df= sale_df.apply(lambda row : format_date_column(row,date_col_name='delivery_date'),axis = 1)
        sale_df['delivery_date'] = sale_df['delivery_date'].apply(lambda x : None if pd.isna(x) else x)


def preprocess_store_table(store_df: pd.DataFrame)-> pd.DataFrame:
        
        store_df = standardize_col_names(store_df)
        store_df = store_df.apply(lambda row : format_date_column(row,date_col_name='open_date'),axis=1)
        #filling online's square meters with 0
        store_df['square_meters'] = store_df['square_meters'].fillna(0) 


def preprocess_exchange_rates_table(df:pd.DataFrame)-> pd.DataFrame:
        #preprocessing exchage table
        df = standardize_col_names(df)
        df = df.apply(lambda row : format_date_column(row,date_col_name='date'),axis=1)




#utility functions
def standardize_col_names(df: pd.DataFrame)->pd.DataFrame:
        col_names = {val:val.lower().replace(' ','_').strip() for val in df.columns}
        df.rename(columns=col_names,inplace=True)
        return df


def format_date_column(row,date_col_name):
    if not pd.isna(row[date_col_name]):
        date = row[date_col_name].split('/')
        row[date_col_name] = f"{date[-1]}-{date[0]}-{date[1]}"
    return row