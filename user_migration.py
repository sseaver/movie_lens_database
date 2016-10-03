import psycopg2
import csv

connection = psycopg2.connect("dbname=movie_lens user=movie_lens")
cursor = connection.cursor()

cursor.execute("drop table if exists userz;")


create_table_command = """
CREATE TABLE public.userz
(
    user_id SERIAL PRIMARY KEY NOT NULL,
    age INT,
    gender VARCHAR(10),
    occupation VARCHAR(50),
    zipcode VARCHAR(10)
);
CREATE UNIQUE INDEX user_user_id_uindex ON public.userz (user_id);
"""


cursor.execute(create_table_command)

with open("ml-100k/u.user.csv", encoding="latin1") as open_file:
    contents = csv.reader(open_file, delimiter='|')

    for row in contents:
        cursor.execute("INSERT INTO userz VALUES (%s,%s,%s,%s,%s);",
                       (row[0], row[1], row[2], row[3], row[4]))


connection.commit()

cursor.close()
connection.close()
