import psycopg2
import csv

connection = psycopg2.connect("dbname=movie_lens user=movie_lens")
cursor = connection.cursor()

cursor.execute("drop table if exists data;")


create_table_command = """
CREATE TABLE public.data
(
    userz_id INT,
    item_id INT,
    rating INT,
    odd_time VARCHAR(50),
    CONSTRAINT data_userz_user_id_fk FOREIGN KEY (userz_id) REFERENCES userz (user_id),
    CONSTRAINT data_item_movie_id_fk FOREIGN KEY (item_id) REFERENCES item (movie_id)
);
"""

cursor.execute(create_table_command)

with open("ml-100k/u.data.csv", encoding="latin1") as data:
    data = csv.reader(data, delimiter='\t')

    for row in data:
        cursor.execute("INSERT INTO data VALUES (%s,%s,%s,%s);",
                       (row[0], row[1], row[2], row[3]))


connection.commit()

cursor.close()
connection.close()
