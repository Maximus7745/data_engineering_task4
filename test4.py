import sqlite3
import pickle
import csv
from test1 import connect_to_db, add_data_into_json, get_top_sorted, get_params, get_freq, filter_data
from test2 import get_data_from_pickle


def get_data_from_csv(file_name):
    data = list()
    with open(file_name, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader, None)
        for row in reader:
            item = {}
            if(len(row) > 0):
                item["name"] = row[0]
                item["method"] = row[1]
                item["param"] = row[2]
                data.append(item)
    
    return data


def try_create_products_table(conn: sqlite3.Connection)-> None:
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS Products (id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    price REAL,
    quantity INTEGER,
    category TEXT,
    fromCity TEXT,
    isAvailable TEXT,
    views INTEGER,
    changes_count INTEGER);""") 
    conn.commit()
    cursor.close()

def try_drop_table(conn: sqlite3.Connection)-> None:
    cursor = conn.cursor()
    cursor.execute("""DROP TABLE IF EXISTS Products;""") 
    conn.commit()
    cursor.close()

def insert_data_into_products(conn: sqlite3.Connection, data: list[dict])-> None:
    cursor = conn.cursor()
    for elem in data:
        if(elem.get("category") is None):
            elem["category"] = "Прочее"
        cursor.execute("""INSERT INTO Products(name, price, quantity, category, fromCity, isAvailable, views, changes_count)
                        VALUES(?, ?, ?, ?, ?, ?, ?, 0);""", (elem.get("name"), elem.get("price"), elem.get("quantity"), elem.get("category"), elem.get("fromCity"), elem.get("isAvailable"), elem.get("views")))
    conn.commit()
    cursor.close()

def hundle_update(conn: sqlite3.Connection, changes: list[dict])-> None:
    s = set()
    for change in changes:
        method = change["method"]
        match method:
            case "remove":
                remove_product(change["name"])
            case "available":
                set_available_product(change["name"], change["param"])
            case "quantity_add":
                add_quantity(change["name"], int(change["param"]))
            case "quantity_sub":
                sub_quantity(change["name"], int(change["param"]))
            case "price_abs":
                change_price_abs(change["name"], float(change["param"]))
            case "price_percent":
                change_price_percent(change["name"], float(change["param"]))

def remove_product(name: str):
    try:
        cursor = conn.cursor()
        cursor.execute(f"""DELETE FROM Products WHERE EXISTS (SELECT * FROM Products WHERE name = ?) AND 
                       name = ?;""", (name, name))
        conn.commit()
    finally:
        cursor.close()

def set_available_product(name: str, param: str):
    try:
        cursor = conn.cursor()
        #cursor.execute(f"""UPDATE Products SET isAvailable = '{param}' WHERE name = '{name}';""")
        #conn.commit()
        cursor.execute(f"""UPDATE Products SET isAvailable = ?, changes_count = changes_count + 1
                       WHERE EXISTS (SELECT * FROM Products WHERE name = ?) AND 
                       name = ?;""", (param, name, name))
        conn.commit()  
    finally:
        cursor.close()

def add_quantity(name: str, num: int):
    try:
        cursor = conn.cursor()
        #cursor.execute(f"""UPDATE Products SET quantity = (quantity + {num}) WHERE name = '{name}';""")
        #quantity = cursor.execute(f"""SELECT quantity FROM Products WHERE name = '{name}';""").fetchone()
        #if(quantity is not None and quantity[0] >= 0):
        #    conn.commit()
        #    cursor.execute(f"""UPDATE Products SET changes_count = changes_count + 1 WHERE name = '{name}';""")
        cursor.execute(f"""UPDATE Products SET quantity = CASE WHEN (quantity + ?) >= 0
                       THEN quantity + ?
                       ELSE quantity END,
                       changes_count = CASE WHEN (quantity + ?) >= 0
                       THEN changes_count + 1
                       ELSE changes_count END WHERE EXISTS (SELECT * FROM Products WHERE name = ?) AND 
                       name = ?;""", (num, num, num, name, name))
        conn.commit()     
    finally:
        cursor.close()

def sub_quantity(name: str, num: int):
    try:
        cursor = conn.cursor()
        #cursor.execute(f"""UPDATE Products SET quantity = quantity + {num} WHERE name = '{name}';""")
        #quantity = cursor.execute(f"""SELECT quantity FROM Products WHERE name = '{name}';""").fetchone()
        #if(quantity is not None and quantity[0] >= 0):
        #    conn.commit()
        #    cursor.execute(f"""UPDATE Products SET changes_count = changes_count + 1 WHERE name = '{name}';""")
        cursor.execute(f"""UPDATE Products SET quantity = CASE WHEN (quantity + ?) >= 0
                       THEN quantity + ?
                       ELSE quantity END,
                       changes_count = CASE WHEN (quantity + ?) >= 0
                       THEN changes_count + 1
                       ELSE changes_count END WHERE EXISTS (SELECT * FROM Products WHERE name = ?) AND 
                       name = ?;""", (num, num, num, name, name))
        conn.commit()        
    finally:
        cursor.close()

def change_price_abs(name: str, num: float):
    try:
        cursor = conn.cursor()
        #cursor.execute(f"""UPDATE Products SET price = price + {num} WHERE name = '{name}';""")
        #price = cursor.execute(f"""SELECT price FROM Products WHERE name = '{name}';""").fetchone()
        #if(price is not None and price[0] > 0):
        #    conn.commit()
        #    cursor.execute(f"""UPDATE Products SET changes_count = changes_count + 1 WHERE name = '{name}';""")
        cursor.execute(f"""UPDATE Products SET price = CASE WHEN (price + ?) > 0
                       THEN price + ?
                       ELSE price END,
                       changes_count = CASE WHEN (price + ?) > 0
                       THEN changes_count + 1
                       ELSE changes_count END WHERE EXISTS (SELECT * FROM Products WHERE name = ?) AND 
                       name = ?;""", (num, num, num, name, name))
        conn.commit()
    finally:
        cursor.close()

def change_price_percent(name: str, percent: float):
    try:
        cursor = conn.cursor()
        #cursor.execute(f"""UPDATE Products SET price = price * (1 + {percent}) WHERE name = '{name}';""")
        ##price = cursor.execute(f"""SELECT price FROM Products WHERE name = '{name}';""").fetchone()
        #if(price is not None and price[0] > 0):
        #    conn.commit()
        #    cursor.execute(f"""UPDATE Products SET changes_count = changes_count + 1 WHERE name = '{name}';""")
        cursor.execute(f"""UPDATE Products SET price = CASE WHEN price * (1 + ?) > 0
                       THEN price * (1 + ?)
                       ELSE price END,
                       changes_count = CASE WHEN price * (1 + ?) > 0
                       THEN changes_count + 1
                       ELSE changes_count END WHERE EXISTS (SELECT * FROM Products WHERE name = ?) AND 
                       name = ?;""", (percent, percent, percent, name, name))
        conn.commit()
    finally:
        cursor.close()

def second_query(conn: sqlite3.Connection)-> list[dict]:
    cursor = conn.cursor()
    results = cursor.execute("""SELECT category, SUM(price) AS sum, MIN(price) AS min, 
                             MAX(price) AS max, AVG(price) AS avg_price, COUNT() AS count FROM Products 
                             GROUP BY category;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))

def third_query(conn: sqlite3.Connection)-> list[dict]:
    cursor = conn.cursor()
    results = cursor.execute("""SELECT category, SUM(quantity) AS sum, MIN(quantity) AS min, 
                             MAX(quantity) AS max, AVG(quantity) AS avg_price FROM Products 
                             GROUP BY category;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))


def fourth_query(conn: sqlite3.Connection)-> list[dict]:
    cursor = conn.cursor()
    results = cursor.execute("""SELECT fromCity, price FROM Products WHERE price > 1000;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))


try:
    data = get_data_from_pickle(r"di-4-data-master\4\task_4_var_24_product_data.pkl")
    add_data_into_json("text.json", data)
    changes = get_data_from_csv(r"di-4-data-master\4\task_4_var_24_update_data.csv")
    conn = connect_to_db("ex4.db")
    try_drop_table(conn)
    try_create_products_table(conn)
    insert_data_into_products(conn, data)
    hundle_update(conn, changes)
    result = dict()
    result['query1'] = get_top_sorted(conn, 'Products','changes_count', 10)
    result['query2'] = second_query(conn)
    result['query3'] = third_query(conn)
    result['query4'] = fourth_query(conn)
    add_data_into_json("result4.json", result)


finally:
    conn.close()
