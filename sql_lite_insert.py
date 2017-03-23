import sqlite3
conn = sqlite3.connect('sqlite3_example.db')

c = conn.cursor()


c.execute('''
          INSERT INTO household_type VALUES(1, 'over65', 75)
          ''')

c.execute('''
          INSERT INTO household_type VALUES(2, 'under65', 90)
          ''')

c.execute('''
          INSERT INTO household VALUES(1, 1)
          ''')

c.execute('''
         INSERT INTO household VALUES(2, 2)
          ''')

c.execute('''
          INSERT INTO household VALUES(3, 1)
          ''')

conn.commit()
conn.close()
