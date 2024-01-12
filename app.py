from flask import Flask
from typing import List
import db

app = Flask(__name__)


@app.get("/country")
def get_countries() -> tuple | List:
    country_list = db.get_all_countries()
    if "error_code" in country_list[0]:
        error_code = country_list[0].pop("error_code")
        return country_list[0], error_code
    else:
        return country_list


@app.get("/country/<int:country_id>")
def get_country(country_id: int) -> tuple | dict:
    country_info = db.get_country_by_id(country_id)

    if "error_code" in country_info:
        error_code = country_info.pop("error_code")
        return country_info, error_code
    else:
        return country_info


@app.get("/city")
def get_cities() -> tuple | List:
    city_list = db.get_all_cities()
    if "error_code" in city_list[0]:
        error_code = city_list[0].pop("error_code")
        return city_list[0], error_code
    else:
        return city_list


@app.get("/city/<int:city_id>")
def get_city(city_id: int) -> tuple | dict:
    city_info = db.get_city_by_id(city_id)
    if "error_code" in city_info:
        error_code = city_info.pop("error_code")
        return city_info, error_code
    else:
        return city_info


@app.get("/product")
def get_products() -> tuple | List:
    product_list = db.get_all_products()
    if "error_code" in product_list[0]:
        error_code = product_list[0].pop("error_code")
        return product_list[0], error_code
    else:
        return product_list


@app.get("/product/<int:product_id>")
def get_product(product_id: int) -> tuple | dict:
    product_info = db.get_product_by_id(product_id)
    if "error_code" in product_info:
        error_code = product_info.pop("error_code")
        return product_info, error_code
    else:
        return product_info


@app.get("/sale")
def get_sales() -> tuple | List:
    sale_list = db.get_all_sales()
    if "error_code" in sale_list[0]:
        error_code = sale_list[0].pop("error_code")
        return sale_list[0], error_code
    else:
        return sale_list


@app.get("/sale/<int:sale_id>")
def get_sale(sale_id: int) -> tuple | dict:
    sale_info = db.get_sale_by_id(sale_id)
    if "error_code" in sale_info:
        error_code = sale_info.pop("error_code")
        return sale_info, error_code
    else:
        return sale_info


if __name__ == "__main__":
    app.run()
