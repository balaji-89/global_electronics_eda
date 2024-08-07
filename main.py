from preprocess import preprocess as pp 
from sql.src import connect_sql_server,create_database,create_table,insert_values

if __name__ == "__main__":

    #loading and preprocessig files
    customers,exchange_rates,products,sales,stores = pp.load_data('data_directory_path')
    customers = pp.preprocess_customer_table(customers)
    exchange_rates = pp.preprocess_exchange_rates_table(exchange_rates)
    products = pp.preprocess_product_table(products)
    sales = pp.preprocess_sale_table(sales)
    stores = pp.preprocess_store_table(stores)

    #create sql table and dumping data
    cursor, con = connect_sql_server(host_name='localhost',user_name='root',password='password')
    create_database(database_name='global_electronics',cursor=cursor)
    for tab in ['customer','store','exchange_rate','product','sales']: 
        create_table(table_name=tab,cursor=cursor)
    con.commit()

    #insert values
    insert_values(cursor,table_name='customer',values=customers.values.tolist())
    con.commit()
