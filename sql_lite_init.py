"""module used to test sqlite..."""


import sqlite3
conn = sqlite3.connect('sqlite3_example.db')

c = conn.cursor()

c.execute('''
          CREATE TABLE household_type
          (id INTEGER PRIMARY KEY ASC, type varchar(250) NOT NULL, paper_propensity INTEGER NOT NULL )
          ''')

c.execute('''
          CREATE TABLE household
          (id INTEGER PRIMARY KEY ASC, type_id INTEGER,
           FOREIGN KEY(type_id) REFERENCES household_type(id))
          ''')


conn.commit()
conn.close()
