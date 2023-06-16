import os
from urllib.parse import quote_plus
import mysql.connector

class Mysqlclient:
    def __init__(self, *args, db_table=None, db=None):
        self.host = os.environ.get('MYSQL_HOST','172.17.0.2')
        self.port = os.environ.get('MYSQL_PORT','3306')
        self.user = os.environ.get('MYSQL_USER','root')
        self.passw = os.environ.get('MYSQL_PASSW','maiz')
        self.db_table = db_table
        self.db = os.environ.get('MYSQL_DB','fcca_1')

    def connection_mysql(self):
        #mydb = mysql.connector.connect(host="localhost",user="yourusername",password="yourpassword")
        mydb = mysql.connector.connect(host=self.host, user=self.user, password=self.passw, database=self.db)
        if mydb:
            return mydb

    def insert_maiz(self, record):
        #print(record)
        db_connection = mysql.connector.connect(host=self.host, user=self.user, password=self.passw, database=self.db)
        db_cursor = db_connection.cursor()
        #conn = self.connection_mysql()
        sql = "INSERT INTO sniim_maiz_1 (producto, fecha, origen, destino, precio_min, precio_max, precio_frec, obs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        #cursorObject = conn.cursor()
        db_cursor.execute(sql, record)
        
        db_connection.commit()
        return True
        #conn.close()

        #val = ("Ram", "CSE", "85", "B", "19")
        #inserted = self.collection.insert_one(document)
        #return True if getattr(inserted, 'inserted_id') else False

    def insert_frutas_hortalizas(self, record):
        db_connection = mysql.connector.connect(host=self.host, user=self.user, password=self.passw, database=self.db)
        db_cursor = db_connection.cursor()
        sql = "INSERT INTO sniim_frutas_hortalizas_1 (producto, fecha, presentacion, origen, destino, precio_min, precio_max, precio_frec, obs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        db_cursor.execute(sql, record)
        db_connection.commit()
        return True
