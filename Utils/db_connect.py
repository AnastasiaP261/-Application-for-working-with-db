import mysql.connector
def db_connect():
        try:
            conn = mysql.connector.connect(user='root', password='',  host='127.0.0.1', database='recruiting')
        except:
            conn = None
        return conn
