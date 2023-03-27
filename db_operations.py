import sqlite3
import pandas as pd


class db_operations:

    # Connect to the db
    def __init__(self):
        self.conn = sqlite3.connect('wealth.db')
        self.cur = self.conn.cursor()



    # Calls create_tables method and commits data to db
    def load_data(self):
        self.create_tables()
        self.csv_to_table('100_richest.csv', 'richest')
        self.csv_to_table('num_billionaires.csv', 'num_billionaires')
        self.conn.commit()


    # Method that creates the table and adds data
    def create_tables(self):
        self.cur.execute('DROP TABLE IF EXISTS richest;')
        self.cur.execute ('''
                CREATE TABLE IF NOT EXISTS richest
                (   
                    Rank INTEGER PRIMARY KEY,
                    name TEXT, net_worth TEXT,
                    bday TEXT, nationality TEXT
                );
                ''')

        self.cur.execute("DROP TABLE IF EXISTS num_billionaires;")
        self.cur.execute ('''
                CREATE TABLE IF NOT EXISTS num_billionaires
                (
                    id INTEGER PRIMARY KEY, 
                    country TEXT, 
                    num_billionaires INTEGER, 
                    billonaire_per_million INTEGER,
                    CONSTRAINT fk_country
                        FOREIGN KEY (country)
                        REFERENCES richest(nationality)
                );
                    ''')

    # Changes csv to dataframe
    # Writes the data stored in the dataframe to sql database
    def csv_to_table(self, filename, tablename):
        data_frame = pd.read_csv(f'backend\\{filename}')
        data_frame.to_sql(tablename, self.conn, if_exists='replace', index=False)
