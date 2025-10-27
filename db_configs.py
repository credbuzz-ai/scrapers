"""
Database configuration and utilities for Twitter scraper
"""

import pymysql
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration loaded from environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT')),
}


class Database:
    def __init__(self, user, password, host, port, database):
        """ Initialize the connection and create the cursor object """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def conn(self):
        return pymysql.connect(
            user=self.user, 
            password=self.password, 
            host=self.host, 
            port=self.port,
            database=self.database,
            charset='utf8mb4'
        )

    def cursor(self, conn):
        """ Return the cursor object """
        return conn.cursor()

    def execute_query(self, query, values=None):
        """ Execute the query """
        conn = self.conn()
        try:
            cursor = self.cursor(conn=conn)
            cursor.execute(query, values)
            conn.commit()
            return cursor
        finally:
            conn.close()

    def executemany_query(self, query, values=None):
        """ Execute the query """
        conn = self.conn()
        try:
            cursor = self.cursor(conn=conn)
            cursor.executemany(query, values)
            conn.commit()
            return cursor
        finally:
            conn.close()

    def fetch_query(self, query, values=None):
        """ Execute a SELECT query and return results """
        conn = self.conn()
        try:
            cursor = self.cursor(conn=conn)
            cursor.execute(query, values)
            results = cursor.fetchall()
            return results
        finally:
            conn.close()


# Initialize database connection
db = Database(
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port'],
    database=DB_CONFIG['database']
)