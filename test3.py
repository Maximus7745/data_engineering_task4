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
                item["artist"] = row[0]
                item["song"] = row[1]
                item["duration_ms"] = row[2]
                item["year"] = row[3]
                item["tempo"] = row[4]
                item["genre"] = row[5]
                item["energy"] = row[6]
                data.append(item)
    
    return data


def try_create_songs_table(conn: sqlite3.Connection)-> None:
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS Songs (id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist TEXT,
    song TEXT,
    duration_ms INTEGER,
    year INTEGER,
    tempo REAL,
    genre TEXT,
    energy REAL);""")
    conn.commit()
    cursor.close()

def insert_data_into_songs(conn: sqlite3.Connection, data: list[object])-> None:
    cursor = conn.cursor()
    cursor.executemany("""INSERT OR IGNORE INTO Songs(artist, song, duration_ms, year, tempo, genre, energy)
                        VALUES(:artist, :song, :duration_ms, :year, :tempo, :genre, :energy);""", data)
    conn.commit()
    cursor.close()

try:
    data= get_data_from_pickle(r"di-4-data-master\3\task_3_var_24_part_1.pkl")
    data += get_data_from_csv(r"di-4-data-master\3\task_3_var_24_part_2.csv")
    conn = connect_to_db("ex4.db")
    #insert_data_into_songs(conn, data)
    result = dict()
    try_create_songs_table(conn)
    result['query1'] = get_top_sorted(conn, 'Songs','year')
    result['query2'] = get_params(conn, 'Songs', 'tempo')
    result['query3'] = get_freq(conn, 'Songs', 'artist')
    result['query4'] = filter_data(conn, 'Songs', min=2010, filtered_column="year", order_column="energy", limit=39)
    add_data_into_json("result3.json", result)


finally:
    conn.close()
