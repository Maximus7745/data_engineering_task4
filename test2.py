import sqlite3
import pickle
from test1 import connect_to_db, add_data_into_json

def get_data_from_pickle(file_name):
    with open(file_name, "rb") as f:
        data = pickle.load(f)
    return data


def try_create_comments_table(conn: sqlite3.Connection)-> None:
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS Comments (id INTEGER PRIMARY KEY AUTOINCREMENT,
    constructions_id INTEGER,
    name TEXT,
    raiting REAL,
    convenience INTEGER,
    security INTEGER,
    comment TEXT);""")
    conn.commit()
    cursor.close()

def insert_data_into_comments(conn: sqlite3.Connection, data: list[object])-> None:
    cursor = conn.cursor()
    cursor.executemany("""INSERT OR IGNORE INTO Comments(constructions_id, name, raiting, convenience, security, comment)
                        VALUES((SELECT id FROM Constructions WHERE Constructions.name = :name),:name, :rating, :convenience, :security, :comment);""", data)
    conn.commit()
    cursor.close()

def first_query(conn: sqlite3.Connection)-> list[dict]:
    #Получаем список коментариев для каждого строения, построенного после 2000 года
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT Comments.name, comment
                            FROM Comments 
                            JOIN Constructions ON Comments.constructions_id = Constructions.id
                            WHERE year > 2000;""").fetchall()
    cursor.close()
    comments = list(map(lambda x: dict(x), results))
    constructions = {}
    for com in comments:
        if(com['name'] not in constructions):
            constructions[com['name']] = list()
            constructions[com['name']].append(com['comment'])
        else:
            constructions[com['name']].append(com['comment'])
    return constructions

def second_query(conn: sqlite3.Connection)-> list[dict]:
    #Получаем средний рейтинг для каждого строения, количество этажей которого меньше 5
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT name, (SELECT AVG(Comments.raiting) AS avg_rating 
                             FROM Comments WHERE Comments.constructions_id = Constructions.id) AS avg_rating
                            FROM Constructions 
                            WHERE floors < 5;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))

def third_query(conn: sqlite3.Connection)-> list[dict]:
    #Получаем года постройки строений, количество коментариев для которых больше 50
    cursor = conn.cursor()
    results = cursor.execute(f"""SELECT name, year
                            FROM Constructions 
                            WHERE (SELECT COUNT(*) FROM Comments 
                            WHERE Comments.constructions_id = Constructions.id) > 100;""").fetchall()
    cursor.close()
    return list(map(lambda x: dict(x), results))

try:
    data = get_data_from_pickle(r"di-4-data-master\2\task_2_var_24_subitem.pkl")
    conn = connect_to_db("ex4.db")
    result = dict()
    try_create_comments_table(conn)
    #insert_data_into_comments(conn, data)
    result['query1'] = first_query(conn)
    result['query2'] = second_query(conn)
    result['query3'] = third_query(conn)
    add_data_into_json("result2.json", result)


finally:
    conn.close()
