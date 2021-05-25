import mysql.connector
from csv import DictReader


def connect():

    my_host = 'localhost'
    my_database = 'goodreads'
    my_user_id = 'root'
    my_password = 'password'

    conn = mysql.connector.connect(host = my_host, user = my_user_id, 
        password = my_password)

    cur = conn.cursor()



    cur.execute('CREATE DATABASE {};'.format(my_database))
    conn2 = mysql.connector.connect(host = my_host, user = my_user_id, 
        password = my_password, database = my_database)

    return conn2

def create_tables(cur, conn):
    cur.execute("""
        CREATE TABLE genres (
            id INT AUTO_INCREMENT PRIMARY KEY,
            genre VARCHAR(100)
        )
    """)

    cur.execute("""
        CREATE TABLE birth_places (
            id INT AUTO_INCREMENT PRIMARY KEY,
            birth_place VARCHAR(255)
        )
    """)

    conn.commit()

    cur.execute(""" 
        CREATE TABLE authors (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            average_rating REAL,
            gender VARCHAR(60),
            birth_place_id INT REFERENCES birth_places(id),
            ratings INT,
            reviews INT
        ) 
    
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE books (
            id INT PRIMARY KEY,
            title VARCHAR(256),
            author_id INT REFERENCES authors(id),
            genre_id INT REFERENCES genres(id),
            pages INT,
            ratings INT,
            reviews INT,
            score INT,
            average_rating REAL

        )
    
    """)
    conn.commit()

def insert_data(cur, conn):

    reader = DictReader(open("goodreads.csv", encoding="utf8"))

    
    for line in reader:


        g = line["genre"].lower()
        cur.execute("SELECT * FROM genres WHERE genre=%s", (g,))
        res = cur.fetchall()
        if(len(res)==0):
            cur.execute("INSERT INTO genres (genre) VALUES (%s)", (g,))
            conn.commit()
        
        cur.execute("SELECT id FROM genres WHERE genre=%s", (g,))
        g_id = cur.fetchone()[0]
        
        
        bp = line["birth_place"].lower()
        cur.execute("SELECT * FROM birth_places WHERE birth_place=%s", (bp,))
        res = cur.fetchall()
        if(len(res)==0):
            cur.execute("INSERT INTO birth_places (birth_place) VALUES (%s)", (bp,))
            conn.commit()
        
        cur.execute("SELECT id FROM birth_places WHERE birth_place=%s", (bp,))
        bp_id = cur.fetchone()[0]

        cur.execute("SELECT * FROM authors WHERE id=%s", (line["author_id"],))
        if(len(cur.fetchall())==0):
            cur.execute("INSERT INTO authors (id, name, average_rating, gender, birth_place_id, ratings, reviews) VALUES (%s, %s, %s, %s, %s, %s, %s)", (line["author_id"], line["author_name"], line["author_average_rating"], line["author_gender"], bp_id, line["author_rating_count"], line["author_review_count"] ))
            conn.commit()

    

        cur.execute("SELECT * FROM books WHERE id=%s", (line["book_id"], ))
        if(len(cur.fetchall())==0):
            cur.execute("INSERT INTO books (id, title, author_id, genre_id, pages, ratings, reviews, score, average_rating) values (%s, %s, %s, %s, %s, %s,%s, %s, %s)", (line["book_id"], line["book_title"], line["author_id"], g_id, line["pages"], line["num_ratings"], line["num_reviews"], line["score"], line["book_average_rating"] ))
            conn.commit()

def main():
    connection = connect()
    cursor = connection.cursor()
    create_tables(cursor, connection)
    insert_data(cursor, connection)

main()


    