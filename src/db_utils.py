import sqlite3

class SQLiteDatabase:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_name)

    def close(self):
        if self.conn:
            self.conn.close()

    def create_table(self, table_name, columns):
        self.connect()
        cursor = None
        try:
            cursor = self.conn.cursor()
            columns_str = ', '.join([f'{col_name} {col_type}' for col_name, col_type in columns.items()])
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})')
            self.conn.commit()
        except Exception as error:
            print("`create_table` Error: {}".format(error))
        finally:
            if cursor:
                cursor.close()
            self.close()

    def drop_table(self, table_name):
        self.connect()
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            self.conn.commit()
        except Exception as error:
            print("`drop_table` Error: {}".format(error))
        finally:
            if cursor:
                cursor.close()
            self.close()

    def insert_multiple_rows(self, table_name, rows):
        self.connect()
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.executemany(f'INSERT INTO {table_name} VALUES ({",".join(["?" for _ in rows[0]])})', rows)
            self.conn.commit()
        except Exception as error:
            print("`insert_multiple_rows` Error: {}".format(error))
        finally:
            if cursor:
                cursor.close()
            self.close()

    def insert_single_row(self, table_name, row):
        self.connect()
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'INSERT INTO {table_name} VALUES ({",".join(["?" for _ in row])})', row)
            self.conn.commit()
        except Exception as error:
            print("`insert_single_row` Error: {}".format(error))
        finally:
            if cursor:
                cursor.close()
            self.close()

    def delete_multiple_rows(self, table_name, condition):
        self.connect()
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'DELETE FROM {table_name} WHERE {condition}')
            self.conn.commit()
        except Exception as error:
            print("`delete_multiple_rows` Error: {}".format(error))
        finally:
            if cursor:
                cursor.close()
            self.close()

    def delete_single_row(self, table_name, condition):
        self.connect()
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'DELETE FROM {table_name} WHERE {condition} LIMIT 1')
            self.conn.commit()
        except Exception as error:
            print("`delete_single_row` Error: {}".format(error))
        finally:
            if cursor:
                cursor.close()
            self.close()

    def fetch_data(self, table_name, columns='*', condition=None):
        self.connect()
        cursor = None
        try:
            cursor = self.conn.cursor()
            if condition:
                cursor.execute(f'SELECT {columns} FROM {table_name} WHERE {condition}')
            else:
                cursor.execute(f'SELECT {columns} FROM {table_name}')
            data = cursor.fetchall()
        except Exception as error:
            print("`fetch_data` Error: {}".format(error))
            data = "ERROR"
        finally:
            if cursor:
                cursor.close()
            self.close()
        return data
    
    def fetch_query(self, query):
        self.connect()
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
        except Exception as error:
            print("`fetch_query` Error: {}".format(error))
            data = "ERROR"
        finally:
            if cursor:
                cursor.close()
            self.close()
        return data
    
    # to update the database table with query 
    def update_query(self,query):
        self.connect()
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(query) 
            self.conn.commit()
        except Exception as error:
            print("`update_query` Error: {}".format(error))
            data = "ERROR"
        finally:
            if cursor:                
                cursor.close()
            self.close()
        #print(" Updated the table with the query--:",query)

ecom_db = SQLiteDatabase("./data/ecom.db")
print("Creating sql DB and inserting Data into it")
