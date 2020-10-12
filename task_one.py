'''
Created Oct 12, 2020

@author: Matt Healy

Purpose: From a list of 15 randomly chosen integers between 1-87 [inclusive],
        return a list of dicts (semi-structured data) of the following format:
        {
            "film": "<Film Title>",
            "character":
            [
                "<Name of Character 1>",
                "<Name of Character 2>",
                "<Name of Character 3>",
            ]
        } 
'''
import requests
import pymysql
import json
from dataclasses import dataclass
from random import sample
from uuid import uuid4

'''
These table names would be environment variables but I chose to specify them in
this script for simplicity and so that everything is in one place
'''
film_tablename = "film"
character_tablename = "person"
relationship_tablename = "relationship"

def main():

    host = "localhost"
    user = "root"
    password = ""
    port = 3308
    db = "starwars_db"

    handler = DBConn(host, user, password, port, db)
    handler.create_db()
    api = StarWarsAPI()
    rand = RandGen() 

    chars_added = []
    while len(chars_added) < 15: # specs ask for 15 random characters
        character_id = rand.new()
        character_response = api.get_character(character_id)

        if character_response.status_code == 200:
            new_char = Character(
                id = character_id,
                name = character_response.json()["name"]
            )
            chars_added.append(new_char)
            new_char.save(handler)
        else:
            continue

        for film in character_response.json()["films"]:
            film_response = api.get(film)

            if film_response.status_code == 200:
                film_id = film[:-1].split("/")[-1]  # Extract ID from URL

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


    with handler.get_connection().cursor() as cur:
        # Group relationship table by film id to get list of characters
        cur.execute(f'''select
                            f.title,
                            GROUP_CONCAT(c.name)
                        from {relationship_tablename} r
                        left join {film_tablename} f
                            on r.film_id = f.id
                        left join {character_tablename} c
                            on r.character_id = c.id
                        group by r.film_id
                    ''')

    data = cur.fetchall()
    return [{'film' : rec[0], 'character' : rec[1].split(',')} for rec in data]


class RandGen:

    def __init__(self, min_num=1, max_num=87):
        '''
        Shuffled list of ints between min_num and max_num
        :param min_num (int): lowest number in list (should always be 1) for this use-case
        :param max_num (int): highest number in list (should always be 87) for this use-case
        '''
        self.min = min_num
        self.max = max_num
        self.bin = sample(range(self.min, self.max), self.max-self.min)

    def new(self):
        # Pop first element from list or return an error if list  is empty
        try:
            new_num = self.bin.pop()
        except IndexError:
            raise IndexError("You're outta numbers")
        else:
            return new_num

class StarWarsAPI:

    def __init__(self):
        self.basepath = "https://swapi.dev/api/"

    def get_character(self, id: int):
        url = self.basepath + f"people/{id}"
        return requests.get(url)

    def get_film(self, id: int):
        url = self.basepath + f"films/{id}"
        return requests.get(url)

    def get(self, url: str):
        return requests.get(url)


class DBConn:

    def __init__(self, host, user, password, port, db):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db = db

    def create_db(self):
        connection = pymysql.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                port = self.port
            )
        try:
            with connection.cursor() as cur:
                cur.execute(f'DROP DATABASE IF EXISTS {self.db}')
                cur.execute(f'CREATE DATABASE {self.db} CHARACTER SET utf8')
        finally:
            connection.close()

    def get_connection(self):
        connection = pymysql.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                port = self.port,
                database = self.db
            )
        return connection

    def make_or_pass(self, tablename: str, schema: dict):
        '''
        Create table if it doesn't already exist
        :param tablename: Name of table
        :param schema: dict of format {<column_name> : <data_type>}
        '''
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
                    for statement in sql.split(';'):
                        cursor.execute(statement)
                        connection.commit()
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
        return character_tablename

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
        return film_tablename

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
    # Table that stores data needed to join Film table to Character table
    id: "VARCHAR(255)"
    character_id: "int"
    film_id: "int"

    @property
    def tablename(self):
        return relationship_tablename

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
    data = main()
    print(json.dumps(data, indent=4, ensure_ascii=False))
