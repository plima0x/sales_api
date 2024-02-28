from psycopg import rows, connect, DatabaseError, ProgrammingError, InterfaceError
import os
from typing import List


def get_dsn() -> str:
    """
    Get connection string to connect to the postgresql database.
    :return: A string representing the database connection.
    """
    db_conn = os.getenv("DB_DSN")
    if not db_conn:
        raise InterfaceError
    return db_conn


# Functions to create the schema:

def create_tables():
    """
    Create all the database tables that will be used with the api.
    :return: None
    """
    tables_created = False
    create_country_sql = """
    CREATE TABLE IF NOT EXISTS tb_countries
    (cod_country  SERIAL NOT NULL,
     desc_country VARCHAR(80) NOT NULL);
    """
    create_city_sql = """
    CREATE TABLE tb_cities
    (cod_city         SERIAL       NOT NULL,
     cod_country      INTEGER      NOT NULL,
     desc_city        VARCHAR(80)  NOT NULL,
     qtd_population   INTEGER      NOT NULL);
    """
    create_product_sql = """
    CREATE TABLE IF NOT EXISTS tb_products
    (cod_product SERIAL       NOT NULL,
     desc_product VARCHAR(80) NOT NULL
    );"""
    create_sale_by_city_sql = """
    CREATE TABLE IF NOT EXISTS tb_sales
    (cod_sale              SERIAL      NOT NULL,
     cod_city              INTEGER     NOT NULL,
     cod_product           INTEGER     NOT NULL,
     qtd_web_purchases     INTEGER     NOT NULL,
     qtd_refunds           INTEGER     NOT NULL
    );
    """
    try:
        with connect(get_dsn()) as conn:
            with conn.cursor() as cursor:
                print("[+] Creating tables")
                cursor.execute(create_country_sql)
                cursor.execute(create_city_sql)
                cursor.execute(create_product_sql)
                cursor.execute(create_sale_by_city_sql)
                print("[+] Tables created!\n")
                tables_created = True

    except ProgrammingError as pge:
        print(f"[!] A programming error occurred while creating the tables: {pge}")
    except DatabaseError as dbe:
        print(f"[!] A database error occurred while creating the tables: {dbe}")
    except InterfaceError:
        print("[!] Error while creating the tables: environment variable DB_DSN not found.")
    return tables_created


def create_constraints():
    """
    Create all the tables constraints.
    :return: None
    """
    constraints_created = False
    create_country_pk = """
    ALTER TABLE tb_countries 
    ADD CONSTRAINT pk_cod_country 
    PRIMARY KEY(cod_country);
    """
    create_country_un = """
    ALTER TABLE tb_countries
    ADD CONSTRAINT un_desc_country 
    UNIQUE(desc_country);
    """
    create_city_pk = """
    ALTER TABLE tb_cities 
    ADD CONSTRAINT pk_cod_city 
    PRIMARY KEY(cod_city);
    """
    create_city_fk = """
    ALTER TABLE tb_cities 
    ADD CONSTRAINT fk_cod_country 
    FOREIGN KEY(cod_country)
    REFERENCES tb_countries(cod_country)
    ON DELETE CASCADE;
    """

    create_city_un = """
    ALTER TABLE tb_cities
    ADD CONSTRAINT un_city 
    UNIQUE (desc_city, cod_country);
    """

    create_product_pk = """
    ALTER TABLE tb_products
    ADD CONSTRAINT Pk_cod_product 
    PRIMARY KEY(cod_product);
    """

    create_product_un = """
    ALTER TABLE tb_products 
    ADD CONSTRAINT un_desc_product 
    UNIQUE(desc_product);
    """
    create_sale_by_city_pk = """
    ALTER TABLE tb_sales
    ADD CONSTRAINT pk_cod_sale 
    PRIMARY KEY(cod_sale);
    """
    create_sale_by_city_city_fk = """
    ALTER TABLE tb_sales 
    ADD CONSTRAINT fk_cod_city 
    FOREIGN KEY(cod_city)
    REFERENCES tb_cities(cod_city)
    ON DELETE CASCADE;
    """

    create_sale_by_city_prod_fk = """
    ALTER TABLE tb_sales 
    ADD CONSTRAINT fk_cod_product 
    FOREIGN KEY(cod_product)
    REFERENCES tb_products(cod_product);
    """
    create_sale_by_city_un = """
    ALTER TABLE tb_sales
    ADD CONSTRAINT un_cod_city
    UNIQUE (cod_city);
    """
    try:
        with connect(get_dsn()) as conn:
            with conn.cursor() as cursor:
                print("[+] Creating constraints")
                cursor.execute(create_country_pk)

                cursor.execute(create_country_un)

                cursor.execute(create_city_pk)

                cursor.execute(create_city_fk)

                cursor.execute(create_city_un)

                cursor.execute(create_product_pk)

                cursor.execute(create_product_un)

                cursor.execute(create_sale_by_city_pk)

                cursor.execute(create_sale_by_city_city_fk)

                cursor.execute(create_sale_by_city_prod_fk)

                cursor.execute(create_sale_by_city_un)

                print("[+] Constraints created\n")
                constraints_created = True



    except ProgrammingError as pge:
        print(f"[!] A programming error occurred while creating constraints: {pge}")

    except DatabaseError as dbe:
        print(f"[!] An database error occurred while creating constraints: {dbe}")

    except InterfaceError:
        print("[!] Error while creating the constraints: environment variable DB_DSN not found.")

    return constraints_created


def get_csv_content(filename) -> List:
    """
    Extract the table data in the csv file.
    :param filename: The csv to extract the data that will be inserted in the database table.
    :return: A list containing the data from the csv.
    """
    content_list = []
    with open(filename) as csv_file:
        for line in csv_file:
            stripped_line = [content.strip("\n\"") for content in line.split(",")]
            content_list.append(stripped_line)

    return content_list[1:]


def insert_data(function_name, insert_query, insert_data_list) -> bool:
    """
    Execute the insert statement in the postgresql database.
    :param function_name: The caller function. This will be used for debugging.
    :param insert_query: the insert statement to be run.
    :param insert_data_list: the parameters to be used in the insert statement.
    :return: True if the operation was successfully. False otherwise.
    """
    data_inserted = False
    try:
        with connect(get_dsn()) as conn:
            with conn.cursor() as cursor:
                for row_data in insert_data_list:
                    cursor.execute(insert_query, row_data)
                data_inserted = True
    except ProgrammingError as pge:
        print(f"[!] A programming error occurred in {function_name} while inserting on table: {pge}")

    except DatabaseError as dbe:
        print(f"[!] An database error occurred in {function_name} while inserting on table: {dbe}")

    except InterfaceError:
        print("[!] Error while inserting into tables: environment variable DB_DSN not found.")

    return data_inserted


def populate_all_tables():
    """
    Call insert_data for every api table to populate them.
    :return: None
    """
    func_name = populate_all_tables.__name__
    country_insert_query = """
    INSERT INTO tb_countries(desc_country)
    VALUES(%s);
    """
    city_insert_query = """
    INSERT INTO tb_cities (cod_country, desc_city, qtd_population)
    VALUES(%s, %s, %s);
    """
    product_insert_query = """
    INSERT INTO tb_products(desc_product)
    VALUES(%s)
    """
    sale_by_city_insert_query = """
    INSERT INTO tb_sales(cod_city, qtd_web_purchases, cod_product, qtd_refunds)
    VALUES(%s, %s, %s, %s);
    """
    insert_items = [("tb_countries_data.csv", country_insert_query),
                    ("tb_cities_data.csv", city_insert_query),
                    ("tb_products_data.csv", product_insert_query),
                    ("tb_sale_by_city.csv", sale_by_city_insert_query)]
    for csv_file, data_query in insert_items:

        data_list = get_csv_content(csv_file)
        print(f"[+] Inserting data from {csv_file}")
        if not insert_data(func_name, data_query, data_list):
            break
        else:
            print("[+] Data inserted\n")


def update_table(function_name, update_query, param_list: List):
    updated_successful = False
    try:
        with connect(get_dsn()) as conn:
            with conn.cursor() as update_cursor:
                update_cursor.execute(update_query, param_list)
                updated_successful = True
    except DatabaseError as dbe:
        print(f"[!] A database error occurred when updating table in {function_name}: {dbe}")

    return updated_successful


def get_many_rows(function_name: str, base_query: str, api_resource: str) -> List[dict]:
    list_of_rows = []
    try:
        with connect(get_dsn(), row_factory=rows.dict_row) as conn:
            with conn.cursor() as cursor:
                cursor.execute(base_query)
                list_of_rows = cursor.fetchall()

    except DatabaseError as dbe:
        print(f"[!] A database error occurred when getting many rows in {function_name}: {dbe}")
        error_dict = {"error_code": 500, "error_msg": f"A server error occurred when getting {api_resource}"}
        list_of_rows.append(error_dict)
    return list_of_rows


def get_one_row(function_name: str, base_query: str, row_parameter: List, api_resource: str) -> dict:
    row = {}
    try:
        with connect(get_dsn(), row_factory=rows.dict_row) as conn:
            with conn.cursor() as cursor:
                cursor.execute(base_query, row_parameter)
                row = cursor.fetchone() or {}
                if not row:
                    row["error_code"] = 404
                    row["error_msg"] = f"No {api_resource} found for the id {row_parameter[0]}"

    except DatabaseError as dbe:
        print(f"[!] A database error occurred when getting row by id in {function_name}: {dbe}")
        row["error_code"] = 500
        row["error_msg"] = f"A server error occurred when getting {api_resource} by id {row_parameter[0]}"

    return row


# Functions called by the api.

def get_all_countries() -> List[dict]:
    get_countries_query = "SELECT cod_country, desc_country FROM tb_countries;"
    api_country_name = "country"
    return get_many_rows(get_all_countries.__name__, get_countries_query, api_country_name)


def get_country_by_id(country_id: int) -> dict:
    get_country_query = "SELECT cod_country, desc_country FROM tb_countries WHERE cod_country = %s;"
    api_country_name = "country"
    return get_one_row(get_country_by_id.__name__, get_country_query, [country_id], api_country_name)


def get_all_cities() -> List[dict]:
    get_cities_query = "SELECT cod_city, desc_city FROM tb_cities;"
    api_city_name = "city"
    return get_many_rows(get_all_cities.__name__, get_cities_query, api_city_name)


def get_city_by_id(city_id: int) -> dict:
    get_city_query = "SELECT cod_city, desc_city FROM tb_cities WHERE cod_city = %s;"
    api_city_name = "city"
    return get_one_row(get_city_by_id.__name__, get_city_query, [city_id], api_city_name)


def get_all_products() -> List[dict]:
    get_products_query = "SELECT cod_product, desc_product FROM tb_products;"
    api_product_name = "product"
    return get_many_rows(get_all_products.__name__, get_products_query, api_product_name)


def get_product_by_id(product_id: int) -> dict:
    get_product_query = "SELECT cod_product, desc_product FROM tb_products WHERE cod_product = %s;"
    api_product_name = "product"
    return get_one_row(get_product_by_id.__name__, get_product_query, [product_id], api_product_name)


def get_all_sales() -> List[dict]:
    get_sales_query = """
    SELECT s.cod_sale, c.cod_city, c.desc_city, p.cod_product, p.desc_product, s.qtd_web_purchases, s.qtd_refunds
    FROM tb_sale_by_city s, tb_cities c, tb_products p
    WHERE s.cod_city = c.cod_city
    AND s.cod_most_sold_prod = p.cod_product
    ORDER BY s.cod_sale;
    """
    api_sale_name = "sale"
    return get_many_rows(get_all_sales.__name__, get_sales_query, api_sale_name)


def get_sale_by_id(sale_id: int):
    get_sale_query = """
    SELECT s.cod_sale, c.cod_city, c.desc_city, p.cod_product, p.desc_product, s.qtd_web_purchases, s.qtd_refunds
    FROM tb_sale_by_city s, tb_cities c, tb_products p
    WHERE s.cod_city = c.cod_city
    AND s.cod_most_sold_prod = p.cod_product
    AND s.cod_sale = %s
    ORDER BY s.cod_sale;
    """
    api_sale_name = "sale"
    return get_one_row(get_sale_by_id.__name__, get_sale_query, [sale_id], api_sale_name)
