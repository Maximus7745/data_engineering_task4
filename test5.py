import sqlite3
import json
import csv
from test1 import connect_to_db, add_data_into_json

#Предметная область: книжный магазин

def get_data_from_csv(file_name):
    data = list()
    with open(file_name, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=",")
        next(reader, None)
        for row in reader:
            item = {}
            if(len(row) > 0):
                item["title"] = row[0]
                item["book_category"] = row[1]
                item["star_rating"] = row[2]
                item["price"] = row[3]
                item["quantity"] = row[5]
                data.append(item)
    
    return data

def get_data_from_json(file_name):
    with open(file_name, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    return data

def try_create_books_tables(conn: sqlite3.Connection)-> None:
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS books (id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    category TEXT,
    stars INTEGER,
    availability INTEGER);""") 
    cursor.execute("""CREATE TABLE IF NOT EXISTS book_prices (id INTEGER PRIMARY KEY AUTOINCREMENT,
    book_id INTEGER,
    price REAL,
    price_excl_tax REAL,
    price_incl_tax REAL,
    tax REAL);""") 
    cursor.execute("""CREATE TABLE IF NOT EXISTS e_books (id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT,
    book_id INTEGER,
    description TEXT,
    num_reviews INTEGER,
    upc TEXT);""") 
    conn.commit()
    cursor.close()

def insert_data_into_tables(conn: sqlite3.Connection, data_books1: list[dict], data_books2: list[dict])-> None:
    cursor = conn.cursor()
    for elem in data_books1:
        cursor.execute("""INSERT INTO books(title, category, stars, availability)
                        VALUES(?, ?, ?, ?);""", (elem.get("title"), elem.get("book_category"), parse_star(elem.get("star_rating")), elem.get("quantity")))
        cursor.execute("""INSERT INTO book_prices(book_id, price)
                        VALUES((SELECT id FROM books WHERE title = ?), ?);""",(elem.get("title"), elem.get("price")))
    for elem in data_books2:       
        cursor.execute("""INSERT INTO books(title, category, stars, availability)
                        VALUES(?, ?, ?, ?);""", (elem.get("title"), elem.get("category"), parse_star(elem.get("stars")), elem.get("availability")))
        cursor.execute(f"""INSERT INTO book_prices(book_id, price, price_excl_tax, price_incl_tax, tax)
                        VALUES((SELECT id FROM books WHERE title = ?), ?, ?, ?, ?);""", \
                        (elem['title'],\
                        elem.get("price"), elem.get("price_excl_tax"), elem.get("price_incl_tax"),elem.get("tax"))) 
        cursor.execute(f"""INSERT INTO e_books(book_id, url, description, num_reviews, upc)
                        VALUES((SELECT id FROM books WHERE title = ? ), ?, ?, ?, ?);""", (\
                        elem['title'],\
                        elem.get("url"), elem.get("description"), elem.get("num_reviews"),elem.get("upc")))     
    conn.commit()
    cursor.close()

def parse_star(star_rating: str):
    match star_rating:
        case "One":
            return 1
        case "Two":
            return 2
        case "Three":
            return 3
        case "Four":
            return 4
        case "Five":
            return 5
        case _:
            return 0

def query1(conn: sqlite3.Connection)-> list[dict]:
    #Получаем 40 книг с лучшим рейтингом
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT title, stars
                             FROM books ORDER BY stars DESC LIMIT 40;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))

def query2(conn: sqlite3.Connection)-> list[dict]:
    #Получаем url книг(если он есть) с ценой больше 50 
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT e_books.url, book_prices.price
                            FROM e_books 
                            JOIN book_prices ON e_books.book_id = book_prices.book_id 
                            WHERE price > 50
                             ;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))

def query3(conn: sqlite3.Connection)-> list[dict]:
    #Получаем суммарную, среднюю и максимальную стоимость всех книг
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT SUM(price) AS sum, MIN(price) AS min, 
                                  MAX(price) as max, AVG(price) as avg FROM book_prices;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))

def query4(conn: sqlite3.Connection)-> list[dict]:
    #Получаем 20 самых низкорейтинговых и дорогих книг(сортируем сначала по рейтингу, а потом по цене)
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT books.title, books.stars, book_prices.price
                            FROM books
                            JOIN book_prices ON books.id = book_prices.book_id 
                            ORDER BY stars ASC, price DESC LIMIT 20;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))

def query5(conn: sqlite3.Connection)-> list[dict]:
    #Получаем общее количество книг с рейтингом равным единице
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT COUNT() AS count
                             FROM books WHERE stars = 1;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))

def query6(conn: sqlite3.Connection)-> list[dict]:
    #Получаем описание 10 книг из категории 'historical fiction'
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT books.title, books.category, e_books.description
                            FROM books
                            JOIN e_books ON books.id = e_books.book_id 
                            WHERE category = 'historical fiction' LIMIT 10;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))


def query7(conn: sqlite3.Connection)-> list[dict]:
    #Увеличим количество книг с рейгингом больше 3 на единицу
    cursor = conn.cursor()
    cursor.execute(f"""UPDATE books SET availability = availability + 1
                       WHERE stars > 3;""")
    cursor.close()


data_table1 = get_data_from_csv(r"data_for_5_task\books_scraped.csv")
data_table2 = get_data_from_json(r"data_for_5_task\Books.json")

try:
    data_books1 = get_data_from_csv(r"data_for_5_task\books_scraped.csv")
    data_books2 = get_data_from_json(r"data_for_5_task\Books.json")
    conn = connect_to_db("ex4.db")
    try_create_books_tables(conn)
    #insert_data_into_tables(conn, data_books1, data_books2)
    result = dict()
    result['query1'] = query1(conn)
    result['query2'] = query2(conn)
    result['query3'] = query3(conn)
    result['query4'] = query4(conn)
    result['query5'] = query5(conn)
    result['query6'] = query6(conn)
    query7(conn)
    add_data_into_json("result5.json", result)


finally:
    conn.close()