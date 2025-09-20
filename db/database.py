import os

import mysql.connector

class Database:
    def __init__(self):
        self.host = os.getenv('MYSQL_HOST')
        self.port = int(os.getenv('MYSQL_PORT', 3306))
        self.user = os.getenv('MYSQL_USER')
        self.password = os.getenv('MYSQL_PASSWORD')
        self.database = os.getenv('MYSQL_DATABASE')
        self.connection = None

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

