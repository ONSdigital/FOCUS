
import sqlite3
conn = sqlite3.connect('sqlite3_example.db')

c = conn.cursor()
c.execute('SELECT * FROM household')
print (c.fetchall())
c.execute('SELECT * FROM household_type')
print (c.fetchall())
conn.close()