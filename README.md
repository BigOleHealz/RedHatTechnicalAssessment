# RedHat Technical Assessment

Make sure you have the correct port specified in line 38!!

In directory, run command `python3 install -r requirements.txt` to install dependencies

To run, simply run command `python3 task_one.py`.

As stated in the comments, the names of the tables should be declared in the environment variables, but they were declared in this script for simplicity for the purposes of this task/challenge

### Character, Film, Relationship classes
Each class represents a table while each instance of the class represents a record to be inserted into that table. I chose to use @dataclasses instead of SQLAlchemy because SQLAlchemy is a pretty large module for such a small task. Each class includes a `.save` method which abstracts away the *INSERT* statement for each record.

### StarWarsAPI class
Pretty self-explanatory. This class declares the API base_url and methods for hitting the character (people) and film endpoints.

### DBConn
Contains methods for creating a db, connecting to that db, and creating the tables if the did not already exist (they should never already exist because theh Database gets dropped and then created before these tables are created)