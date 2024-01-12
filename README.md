                                                  _                         _ 
                                                 | |                       (_)
                                        ___  __ _| | ___  ___    __ _ _ __  _ 
                                       / __|/ _` | |/ _ \/ __|  / _` | '_ \| |
                                       \__ \ (_| | |  __/\__ \ | (_| | |_) | |
                                       |___/\__,_|_|\___||___/  \__,_| .__/|_|
                                                                     | |      
                                                                     |_|      


## What is it?

**Sales api** is a example of rest api with flask.

The api provides the sales by city that belongs to a fictional company. 

It uses the [psycopg3](https://www.psycopg.org/psycopg3/docs/) as a library to connect to the postgresql database. 

And the [flask](https://flask.palletsprojects.com/en/3.0.x/) framework to make the api endpoints.

## Requirements:

Install the postgresql database: 

https://www.postgresql.org/download/

I recommend that you create a [virtualenv](https://docs.python.org/3/library/venv.html) to work with. 

This will install the python packages only in that directory and not in system wide.

The common pattern is to name the virtual env directory as .venv:

```

python3 -m venv .venv

```

Next, install the required modules using pip:

```

pip install -r requirements.txt 

```

You need to create a environment variable with the name **DB_DSN**. This will store the string required to connect to the database.

The environment variable will have the value: **'dbname=you_db_name user=your_db_user host=your_server_ip_addr port=your_server_port password=your_db_password'**.

Substitute the values after the "=" sign with your own database information.

## Getting started:

Run the development server:

```

flask run

```

## Current available api endpoints: 


#### Get all countries where the sale was made: 

```
localhost:5000/country
```

#### Get country by id:

```
localhost:5000/country/<country_id>
```

#### Get all cities where the sales was made:

```
localhost:5000/city 
```

#### Get city by id:

```
localhost:5000/city/<city_id>
```

#### Get all products that was sold: 

```
localhost:5000/product
```

#### Get product by id:

```
localhost:5000/product/<product_id>
```

#### Get all sales by city:

```
localhost:5000/sale
```

#### Get sale by id:

```
localhost:5000/sale/<sale_id> 
```
