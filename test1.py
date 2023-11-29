import sqlite3
import json


def parse_text(file_name: str)-> list[object]:
    with open(file_name,'r', encoding='utf-8') as f:
        lines = f.readlines()
    items = list()
    item = dict()
    for line in lines:
        if line != "=====\n":
            elem = line.strip().split("::")
            item[elem[0]] = elem[1]
        else:
            items.append(item)
            item = dict()
    return items

def connect_to_db(file_name: str)-> sqlite3.Connection:
    conn = sqlite3.connect(file_name)
    conn.row_factory = sqlite3.Row
    return conn

def try_create_table(conn: sqlite3.Connection)-> bool:
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS Constructions (id INTEGER PRIMARY KEY AUTOINCREMENT,
    constructions_id INTEGER UNIQUE,
    name TEXT,
    street TEXT,
    city TEXT,
    zipcode INTEGER,
    floors INTEGER,
    year INTEGER,
    parking TEXT,
    prob_price INTEGER,
    views INTEGER);""")
    conn.commit()
    cursor.close()
    return True

def insert_data(conn: sqlite3.Connection, data: list[object])-> None:
    cursor = conn.cursor()
    cursor.executemany("""INSERT OR IGNORE INTO Constructions(constructions_id, name, street, city, zipcode,
    floors, year, parking, prob_price, views) VALUES(:id, :name, :street, :city, :zipcode,
    :floors, :year, :parking, :prob_price, :views);""", data)
    conn.commit()
    cursor.close()

def get_top_sorted(conn: sqlite3.Connection, table: str, colum_name: str, limit: int = 34)-> list[dict]:
    cursor = conn.cursor()
    rows = cursor.execute(f"""SELECT * FROM {table} ORDER BY {colum_name} DESC LIMIT {limit};""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), rows))

def get_params(conn: sqlite3.Connection, table: str, colum_name: str)-> dict:
    cursor = conn.cursor()
    results = dict(cursor.execute(f"""SELECT SUM({colum_name}) AS sum, MIN({colum_name}) AS min, 
                                  MAX({colum_name}) as max, AVG({colum_name}) as avg FROM {table};""").fetchone())
    cursor.close()
    return results

def get_freq(conn: sqlite3.Connection, table: str, colum_name: str)-> list[dict]:
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT (CAST(COUNT(*) AS REAL) / (SELECT COUNT(*) FROM {table})) 
                             AS count, {colum_name} FROM {table} GROUP BY {colum_name};""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))


def filter_data(conn: sqlite3.Connection, table: str, order_column: str, filtered_column: str , min: int, limit: int = 34)-> list[dict]:
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT * FROM {table} WHERE {filtered_column} > {min} ORDER BY {order_column} DESC LIMIT {limit};""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))



def add_data_into_json(file_name, data):
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)







try:
    start_data = parse_text("di-4-data-master/1/task_1_var_24_item.text")
    conn = connect_to_db("ex4.db")
    data = dict()
    if(try_create_table(conn)):
        insert_data(conn, start_data)
        data['query1'] = get_top_sorted(conn, 'Constructions','year')
        data['query2'] = get_params(conn, 'Constructions', 'views')
        data['query3'] = get_freq(conn, 'Constructions', 'floors')
        data['query4'] = filter_data(conn, 'Constructions', min=1800, filtered_column="year", order_column="prob_price")
        add_data_into_json("result1.json", data)
finally:
    conn.close()
