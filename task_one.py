import requests
import pymysql
from dataclasses import dataclass
from random import sample
from uuid import uuid4

def main():

    host = "localhost"
    user = "root"
    password = ""
    port = 3308
    db = "starwars_db"

    # Create db name starwars_db if it does not already exist
    conn = pymysql.connect(host=host, user=user, password=password, port=port)
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE IF NOT EXISTS {db} CHARACTER SET utf8')
    finally:
        conn.close()

    handler = DBConn(host, user, password, port, db)
    api = StarWarsAPI()
    rand = RandGen() 

    chars_added = []
    while len(chars_added) < 15:
        character_id = rand.new()
        character_response = api.get_character(character_id)

        if character_response.status_code == 200:
            new_char = Character(
                id = character_id,
                name = character_response.json()["name"]
            )
            new_char.save(handler)
        else:
            continue

        for film in character_response.json()["films"]:
            film_response = api.get(film)

            if film_response.status_code == 200:
                film_id = film[:-1].split("/")[-1]

                new_film = Film(
                    id = film_id,
                    title = film_response.json()["title"]
                )
                new_film.save(handler)
            else:
                continue

            # Create relationship
            new_relationship = Relationship(
                id="",
                character_id = character_id,
                film_id = film_id
            )

            new_relationship.save(handler)

class RandGen:

    def __init__(self, min_num=0, max_num=100):
        self.min = min_num
        self.max = max_num
        self.bin = sample(range(self.min, self.max), self.max-self.min)

    def new(self):
        try:
            new_num = self.bin.pop()
        except IndexError:
            raise IndexError("You're outta numbers")
        else:
            return new_num

# Too lazy for ABC
class StarWarsAPI:

    def __init__(self):
        self.basepath = "https://swapi.dev/api/"

    def get_character(self, id):
        url = self.basepath + f"people/{id}"
        return requests.get(url)

    def get_film(self, id):
        url = self.basepath + f"films/{id}"
        return requests.get(url)

    def get(self, url):
        return requests.get(url)


class DBConn:

    def __init__(self, host, user, password, port, db):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db = db

    def get_connection(self):

        connection = pymysql.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                port = self.port,
                database = self.db
            )
        return connection

    def make_or_pass(self, tablename, schema):
        sql = f"select count(*) from information_schema.tables where table_name = '{tablename}'"
        connection = self.get_connection()

        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                response = cursor.fetchone()[0]

                if response == 1:
                    return True
                elif response == 0:
                    fields = [f"{field_name} {field_type}" for field_name, field_type in schema.items()]
                    sql = f"CREATE TABLE {tablename}(" + ", ".join(fields) + ", PRIMARY KEY (id))"
                    cursor.execute(sql)
                else:
                    raise IndexError("non 0 or 1 value returned to table name")
        finally:
            connection.close()


@dataclass
class Character:

    id: "int"
    name: "varchar(255)"

    @property
    def tablename(self):
        return "person"

    def save(self, dbhandler):
        dbhandler.make_or_pass(self.tablename, self.__annotations__)

        sql = f"insert ignore into {self.tablename} (id, name) values (%s, %s)"
        connection = dbhandler.get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, (self.id, self.name))
            connection.commit()
        finally:
            connection.close()

@dataclass
class Film:

    id: "int"
    title: "varchar(255)"

    @property
    def tablename(self):
        return "film"

    def save(self, dbhandler):
        dbhandler.make_or_pass(self.tablename, self.__annotations__)

        sql = f"insert ignore into {self.tablename} (id, title) values (%s, %s)"
        connection = dbhandler.get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, (self.id, self.title))
            connection.commit()
        finally:
            connection.close()

@dataclass
class Relationship:

    id: "VARCHAR(255)"
    character_id: "int"
    film_id: "int"

    @property
    def tablename(self):
        return "relationship"

    def save(self, dbhandler):
        dbhandler.make_or_pass(self.tablename, self.__annotations__)

        sql = f"insert ignore into {self.tablename} (id, character_id, film_id) values (%s, %s, %s)"
        connection = dbhandler.get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, (str(uuid4()), self.character_id, self.film_id))
            connection.commit()
        finally:
            connection.close()

if __name__ == "__main__":
    main()