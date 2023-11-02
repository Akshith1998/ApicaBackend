import pymysql
from pymysql import connect
from apicaproject.settings import DATABASES
 
 
class PyMYSQLClient:
 
    def __init__(self, db_name):
        self.__mConnection = None
        self.__mConnectionType = None
        self.__mCursor = None
        self.__mServerConfig = None
        self.__mDbName = db_name
        self.__mQuery = None
        self.__mQueryResults = None
        self.__mResultIndex = 0
        self.__resultStatus = None
 
    def __del__(self):
        self.close()
 
    def read_query(self, query, fetch_one=False):
        self.__create_read_connection()
        self.__mQuery = query
        if self.__mCursor is None:
            self.__mCursor = self.__mConnection.cursor(pymysql.cursors.DictCursor)
        self.__execute_query(fetch_one=fetch_one)
 
    def write_query(self, query, data_list=None):
        try:
            self.__create_write_connection()
            self.__mQuery = query
            if self.__mCursor is None:
                self.__mCursor = self.__mConnection.cursor()
            self.__execute_query(data_list)
        except Exception as e:
            raise Exception("Exception Occurred at sql for : <" + query + "> With Exception :" + str(e))
 
    def get_item(self):
        current_pointer = self.__mResultIndex
        length = len(self.__mQueryResults)
        if current_pointer >= length:
            return None
        self.__mResultIndex += 1
        return self.__mQueryResults[current_pointer]
 
    def get_all_items(self):
        if self.__mQueryResults:
            self.__mResultIndex = len(self.__mQueryResults)
        return self.__mQueryResults
 
    def select_db(self, db_name):
        self.close()
        self.__mDbName = db_name
 
    def inserted_id(self):
        return self.__mConnection.insert_id()
 
    def affected_rows(self):
        return self.__mConnection.affected_rows()
 
    def escape(self, in_str):
        out_str = ""
        self.__create_read_connection()
        try:
            out_str = self.__mConnection.escape_string(str(in_str, 'utf-8'))
        except:
            out_str = self.__mConnection.escape_string(in_str)
        self.close()
        return out_str
 
    def get_query_status(self):
        return self.__resultStatus
 
    def close(self):
        if self.__mConnection is not None and self.__mConnection.open:
            self.__mConnection.close()
 
    def __create_read_connection(self):
 
        if self.__mConnection is not None and self.__mConnection.open:
            if self.__mConnectionType == 0:
                self.__mConnection.select_db(self.__mDbName)
                return
            else:
                self.close()
        self.__mConnectionType = 0
        self.__mServerConfig = DATABASES["default"]
        self.__create_new_connection()
 
    def __create_write_connection(self):
 
        if self.__mConnection is not None and self.__mConnection.open:
            if self.__mConnectionType == 1:
                self.__mConnection.select_db(self.__mDbName)
                return
            else:
                self.close()
 
        self.__mConnectionType = 1
        self.__mServerConfig = DATABASES["default"]
        self.__create_new_connection()
 
    def __create_new_connection(self):
 
        self.__mConnection = connect(user=self.__mServerConfig.get('USER'),
                                     password=self.__mServerConfig.get('PASSWORD'),
                                     host=self.__mServerConfig.get('HOST'),
                                     port=self.__mServerConfig.get('PORT'),
                                     database=self.__mDbName,
                                     autocommit=True)
 
    def __execute_query(self, fetch_one=False, data_list=None):
        self.__resultStatus = None
        if data_list is None:
            self.__mCursor.execute(self.__mQuery)
        else:
            self.__mCursor.executemany(self.__mQuery, data_list)
        if fetch_one:
            self.__mQueryResults = self.__mCursor.fetchone()
        else:
            self.__mQueryResults = self.__mCursor.fetchall()
        self.__mCursor.close()
        self.__mCursor = None
        self.__mResultIndex = 0
        self.__resultStatus = True