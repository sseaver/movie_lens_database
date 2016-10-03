import psycopg2
import csv

connection = psycopg2.connect("dbname=movie_lens user=movie_lens")
cursor = connection.cursor()

cursor.execute("drop table if exists item;")


create_table_command = """
CREATE TABLE public.item
(
    movie_id SERIAL PRIMARY KEY NOT NULL,
    movie_title VARCHAR(500),
    release_date VARCHAR(20),
    video_release_date VARCHAR(20),
    imdb_url VARCHAR(500),
    unknown BOOLEAN,
    Action BOOLEAN,
    Adventure BOOLEAN,
    Animation BOOLEAN,
    Childrens BOOLEAN,
    Comedy BOOLEAN,
    Crime BOOLEAN,
    Documentary BOOLEAN,
    Drama BOOLEAN,
    Fantasy BOOLEAN,
    FilmNoir BOOLEAN,
    Horror BOOLEAN,
    Musical BOOLEAN,
    Mystery BOOLEAN,
    Romance BOOLEAN,
    SciFi BOOLEAN,
    Thriller BOOLEAN,
    War BOOLEAN,
    Western BOOLEAN
);
CREATE UNIQUE INDEX item_movie_id_uindex ON public.item (movie_id);
"""

cursor.execute(create_table_command)

with open("ml-100k/u.item.csv", encoding="latin1") as item:
    item = csv.reader(item, delimiter='|')

    for row in item:
        cursor.execute("""INSERT INTO item VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""",
                       (row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                        row[7], row[8], row[9], row[10], row[11], row[12], row[13],
                        row[14], row[15], row[16], row[17], row[18], row[19],
                        row[20], row[21], row[22], row[23]))


connection.commit()

cursor.close()
connection.close()
