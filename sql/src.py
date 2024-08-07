
import mysql.connector

TABLE_ATTRIBUTES = {'customer': """(customer_key int,gender varchar(10) NOT NULL,name varchar(50) NOT NULL,city varchar(50) NOT NULL, state_code varchar(30) NOT NULL,
                                    state varchar(50) NOT NULL,zip_code varchar(20) NOT NULL,country varchar(30) NOT NULL,continent varchar(30) NOT NULL,
                                    birthday date, PRIMARY KEY(customer_key))""",
                    
                    'product_category': """(category_id int, sub_category varchar(50) NOT NULL, category varchar(40) NOT NULL,PRIMARY KEY(category_id))""",

                    'product' : """(product_key int,name varchar(100) NOT NULL,brand varchar(30) NOT NULL,color varchar(20) NOT NULL,unit_cost decimal(7,2) NOT NULL,
                                    unit_price_usd decimal(7,2) NOT NULL,category_id int NOT NULL,PRIMARY KEY(product_key),FOREIGN KEY(category_id) REFERENCES product_category(category_id))""",
                    
                    'sale' : """(order_name int,line_item int NOT NULL,order_date date NOT NULL,delivery_date date, customer_key int NOT NULL,store_key int NOT NULL,
                                product_key int NOT NULL,quantity int NOT NULL,currency_code varchar(6) NOT NULL,PRIMARY KEY(order_name, line_item),FOREIGN KEY(customer_key) REFERENCES customer(customer_key),
                                FOREIGN KEY(store_key) REFERENCES store(store_key), FOREIGN KEY(product_key) REFERENCES product(product_key))""",

                    'store' : """(store_key int,country varchar(20) NOT NULL,state varchar(50) NOT NULL, square_meters Decimal(10,2) NOT NULL, open_date date NOT NULL,
                                    PRIMARY KEY(store_key))""",

                    'exchange_rate' : """(date date,currency varchar(7),exchange decimal(7,4))"""
                    
                    }


TABLE_COL_NAMES = {
    'customer': """(customer_key,gender,name,city,state_code,state,zip_code,country,continent,birthday)""",
                    
    'product_category': """(category_id,sub_category,category)""",

    'product' : """(product_key,name,brand,color,unit_cost,unit_price_usd,category_id)""",
                    
    'sale' : """(order_name,line_item,order_date,delivery_date,customer_key,store_key,product_key,quantity,currency_code)""",

    'store' : """(store_key,country,state,square_meters,open_date)""",

    'exchange_rate' : """(date,currency,exchange)"""
}


def connect_sql_server(host_name: str, user_name: str, password: str):
    """
    Connects to the MySQL server.

    Parameters:
        host_name (str): The hostname of the MySQL server.
        user_name (str): The username for the MySQL server.
        password (str): The password for the MySQL server.

    Returns:
        tuple: A tuple containing the MySQL cursor and connection objects.
    """
    try: 
        con = mysql.connector.connect(
        host=host_name,
        user=user_name,
        password=password
        )
        cursor = con.cursor()
        return cursor,con
    except Exception as e:
        raise e


def create_database(database_name: str, cursor, use_database = True)-> None:
    """
    Creates a database if it does not already exist.

    Parameters:
        database_name (str): The name of the database to create.
        cursor (MySQLCursor): The MySQL cursor object.
        use_database (bool): Whether to switch to the new database after creation.
    """
    try:
        query = f"create database {database_name}"
        cursor.execute(query)
        if use_database: 
            cursor.execute(f'use {database_name}')
    except Exception as e:
        raise e


def create_table(cursor,table_name:str):
    """
    Creates a table with specified attributes if it does not already exist.

    Parameters:
        cursor (MySQLCursor): The MySQL cursor object.
        table_name (str): The name of the table to create.
        attributes (str): The attributes of the table in SQL syntax.

    Raises:
        Exception: If an error occurs during table creation.
    """
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} {TABLE_ATTRIBUTES[table_name]}''' )


def insert_values(cursor, table_name:str,values:list):
    """
    Inserts multiple values into a specified table.

    Parameters:
        cursor (MySQLCursor): The MySQL cursor object.
        table_name (str): The name of the table to insert values into.
        attributes (tuple): The attributes of the table as a tuple.
        values (list): The list of values to insert.
    """
    attributes = TABLE_COL_NAMES[table_name]

    cursor.executemany(f'''INSERT INTO {table_name} {attributes} VALUES {str(('%s')*len(eval(attributes)))}''', values)
