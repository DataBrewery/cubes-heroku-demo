# -*- coding=utf -*-
"""Prepare slicer demo data

Taken from cubes-examples.

Author: Jose Juan Montes
Changes: Stefan Urbanek
"""

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import Column, Integer, String, Numeric

import sys
import re
import datetime
import csv
import random

cache = {}
engine = create_engine("sqlite:///webshop.sqlite")
metadata = MetaData(bind=engine)

METADATA = {
    "country": [
        ("continent_id", String),
        ("continent_label", String),
        ("country_id", String),
        ("country_label", String)
    ],

    "customer": [
        ("name", String),
    ],

    "product": [
        ("category_id", String),
        ("category_label", String),
        ("product_id", String),
        ("product_label", String)
    ],

    "country": [
        ("continent_id", String),
        ("continent_label", String),
        ("country_id", String),
        ("country_label", String)
    ],

    "date": [
        ("year", Integer),
        ("quarter", Integer),
        ("month", Integer),
        ("day", Integer),
        ("week", Integer),
        ("weeknum", Integer),
    ],
    # Facts
    "sales": [
        ("date_id", Integer),
        ("customer_id", Integer),
        ("product_id", Integer),
        ("country_id", Integer),

        ("quantity", Numeric),
        ("price_total", Numeric)
    ],
    "webvisits": [
        ("date_id", Integer),
        ("country_id", Integer),

        ("browser", String),
        ("newsletter", String),
        ("source_id", String),
        ("source_label", String),

        ("pageviews", Integer),
    ],
}

tables = {}

def main():

    for name, fields in METADATA.items():
        table = Table(name, metadata, autoload=False)
        if table.exists():
            table.drop(engine)

        table = Table(name, metadata, autoload=False)
        table.append_column(Column("id", String))

        for field_name, type_ in fields:
            table.append_column(Column(field_name, type_))

        tables[name] = table

    metadata.create_all()

    # Import facts and dimension data
    import_sales()

    # Generate all dates
    generate_dates()

    # Generate site visits
    generate_webvisits()

    # Add extra dimensions for left joins
    # insert_product ("Books", "200 ways of slicing a cube")

def save_object(table_name, row):

    if (table_name in cache):
        if (row["id"] in cache[table_name]):
            return row["id"]
    else:
        cache[table_name] = {}

    table = tables[table_name]
    insert = table.insert().values(row)
    engine.execute(insert)

    cache[table_name][row["id"]] = row

    return row["id"]

def sanitize(value):
    if (value == ""):
        value = "(BLANK)"
    elif (value == None):
        value = "(NULL)"
    else:
        value = re.sub('[^\w]', "_", value.strip())
    return value

def insert_country(continent, country):

    record = {
           "id": sanitize(continent + "/" + country),
           "continent_id": sanitize(continent),
           "continent_label": continent,
           "country_id": sanitize(country),
           "country_label": country,
    }
    return save_object("country", record)

def insert_product (category, product):

    row = {
           "id": sanitize (category + "/" + product),
           "category_id": sanitize(category),
           "category_label": category,
           "product_id": sanitize(product),
           "product_label": product,
    }
    return save_object("product", row)

def insert_customer(customer_name):

    row = {
           "id": sanitize(customer_name),
           "name": customer_name
    }
    return save_object("customer", row)

def insert_date(year, month, day):

    row = { }

    date = datetime.date(int(year), int(month), int(day));

    if date != None:
        row["id"] = sanitize(datetime.datetime.strftime(date, "%Y%m%d"))
        row["year"] = date.year
        row["quarter"] = ((date.month - 1) / 3) + 1
        row["month"] = date.month
        row["day"] = date.day
        row["weeknum"] = date.isocalendar()[1]

        start = date - datetime.timedelta(days = date.weekday())
        end = start + datetime.timedelta(days = 6)

        row["week"] = end.strftime("%Y%m%d")

    return save_object ("date", row)

def insert_sale(fact):
    return save_object ("sales", fact)

def insert_webvisit(fact):
    return save_object ("webvisits", fact)

def generate_dates():
    start_date =  datetime.datetime.strptime("2012-01-01", "%Y-%m-%d")
    end_date =  datetime.datetime.strptime("2013-12-31", "%Y-%m-%d")

    cur_date = start_date
    while (cur_date <= end_date):
        insert_date (cur_date.year, cur_date.month, cur_date.day)
        cur_date = cur_date + datetime.timedelta(days = +1)

def import_sales():

    count = 0
    header = None
    with open('data/webshop-facts.csv', 'rb') as f:
        reader = csv.reader(f)
        header = reader.next()

        for row in reader:

            count = count + 1
            fact = { "id" : count }

            record = dict(zip(header, row))

            # Process row
            date_id = "%04d%02d%02d" % (int(record["date_created.year"]),
                                        int(record["date_created.month"]),
                                        int(record["date_created.day"]))
            fact["date_id"] = date_id
            fact["country_id"] = insert_country(record["country.region"],
                                                record["country.country"])
            fact["customer_id"] = insert_customer(record["customer.name"])
            fact["product_id"] = insert_product(record["product.category"],
                                                record["product.name"])

            # Import figures (quick hack for localization issues):
            fact["quantity"] = float(str(record["quantity"]).replace(",", "."))
            fact["price_total"] = float(str(record["price_total"]).replace(",", "."))

            insert_sale(fact)

    print "Imported %d facts " % (count)

def generate_webvisits():

    for i in range (1, 1079):

        fact = { "id" : i }

        fact["country_id"] = random.choice(cache["country"].keys())
        fact["date_id"] = random.choice(cache["date"].keys())

        fact["browser"] = random.choice (["Lynx", "Firefox", "Firefox", "Chrome", "Chrome", "Chrome"])
        fact["newsletter"] = random.choice(["Yes", "No", "No", "No"])

        fact["source_label"] = random.choice(["Web search", "Web search", "Direct link", "Unknown"])
        fact["source_id"] = sanitize(fact["source_label"])

        fact["pageviews"] = abs(int(random.gauss(7, 6))) + 1

        insert_webvisit(fact)

if __name__ == "__main__":
    main()

