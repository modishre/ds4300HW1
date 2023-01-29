"""
filename: dbutils.py - modified
Requires the driver:  pymysql
    - ran into issues with mysql-connector - conda install pymysql
description: A collection of database utilities to make it easier
to implement a database application
"""

import pymysql as mysql
import pandas as pd


class DBUtils:

    def __init__(self, user, password, database, host="localhost"):
        """ Future work: Implement connection pooling """
        self.con = mysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

    def close(self):
        """ Close or release a connection back to the connection pool """
        self.con.close()
        self.con = None

    def get_rows(self, query):
        rs = self.con.cursor()

        # Step 2: Execute the query
        rs.execute(query)

        # Step 3: Get the resulting rows and column names
        rows = rs.fetchall()

        # Step 4: Close the cursor
        rs.close()

        return rows

    def get_data(self, query):
        """Returns raw data from a given query"""
        # create cursor and execute query
        rs = self.con.cursor()
        rs.execute(query)
        data = rs.fetchall()  # fetch raw data
        rs.close()  # Step 4: Close the cursor
        return data

    def get_frame(self, query, cols):
        """ Execute a select query and returns the result as a dataframe """
        # Step 1: Create cursor and execute query
        rs = self.con.cursor()
        rs.execute(query)

        # Step 3: Get the resulting rows and column names
        rows = rs.fetchall()
        frame = pd.DataFrame(rows, columns=cols)

        # Step 4: Close the cursor
        rs.close()

        # Step 5: Return result
        return frame

    def insert_one(self, sql, val):
        """ Insert a single row """
        cursor = self.con.cursor()

        query = sql + val + ";"
        cursor.execute(query)
        self.con.commit()

    def insert_many(self, sql, vals):
        """ Insert multiple rows """
        cursor = self.con.cursor()
        cursor.executemany(sql, vals)
        self.con.commit()
