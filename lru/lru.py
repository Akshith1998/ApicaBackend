import time
import json
import logging
import datetime
from apicaproject.utils import ApiResponse
from apicaproject.mysql import PyMYSQLClient
from datetime import datetime
from collections import deque

logger = logging.getLogger(__name__)

class LRU(PyMYSQLClient):
    def __init__(self, maxKeys=1024):
        self.maxKeys = maxKeys
        PyMYSQLClient.__init__(self,"lru")

    def create_table(self):
        response = ApiResponse(data_type={})
        try:
            query = f"""
                        CREATE TABLE IF NOT EXISTS `Cache` (
                        `key` INT PRIMARY KEY,
                        `value` INT,
                        `expiryTime` REAL,
                        `updatedOn` REAL
                    )
                    """
            self.write_query(query)
            self.close()
        except Exception as error:
            response.handel_exception(logger,'create table error',error,db_err=False)

    def readable_date(self,dt):
        if isinstance(dt, datetime):
            dt = str(dt)
        pattern = "%d %B, %Y"
        if pattern:
            try:
                dt = datetime.strptime(dt)
            except Exception:
                dt = None
        return dt

    def remove(self):
        response = ApiResponse(data_type={})
        try:
            selectQuery = f"""
                        SELECT `key` FROM `Cache` 
                        ORDER BY `updatedOn`
                        LIMIT 1
                    """
            self.read_query(selectQuery)
            self.close()
            db_result = self.get_item()
            if db_result:
                try:
                    deleteQuery = f"""
                                    DELETE FROM `Cache` WHERE `key` = {db_result.get("key")}
                                """
                    self.write_query(deleteQuery)
                    self.close()
                except Exception as error:
                    response.handel_exception(logger,'remove from table error',error,db_err=False)
        except Exception as error:
            response.handel_exception(logger,'remove from table error',error,db_err=False)

    def cleanup_expired_keys(self):
        response = ApiResponse(data_type={})
        try:
            current_time = time.time()
            deleteQuery = f"""
                            DELETE FROM `Cache` WHERE `expiryTime` < {current_time}
                        """
            self.write_query(deleteQuery)
            self.close()
        except Exception as error:
            response.handel_exception(logger,'cleanup expired keys',error,db_err=False)

    def get(self, key):
        response = ApiResponse(data_type={})
        self.cleanup_expired_keys()
        if key:
            try:
                query = f"""
                        SELECT `key`,`value`,`expiryTime` FROM `Cache` where `key`={key}
                    """
                self.read_query(query)
                self.close()
                db_result = self.get_item()
                if db_result:
                    try:
                        updateQuery = f"""
                                        UPDATE `Cache`
                                        SET `updatedOn`={time.time()}
                                        WHERE `key`={key}
                                    """
                        self.write_query(updateQuery)
                        self.close()
                        return db_result.get("value")
                    except Exception as error:
                        response.handel_exception(logger,'get values',error,db_err=False)
            except Exception as error:
                response.handel_exception(logger,'get values',error,db_err=False)
        return None

    def set(self, data):
        response = ApiResponse(data_type={})
        key = int(data.get("key"))
        value = int(data.get("value"))
        expiryTime = int(data.get("expiryTime"))
        self.create_table()
        self.cleanup_expired_keys()
        try:
            query = f"""
                        SELECT `key`,`value`,`expiryTime` FROM `Cache`
                        ORDER BY `updatedOn` DESC LIMIT 3
                    """
            self.read_query(query)
            self.close()
            db_result = list(self.get_all_items())
            if db_result and len(db_result) >= self.maxKeys:
                self.remove()
            try:
                insertQuery = f"""
                                INSERT INTO `Cache` (`key`,`value`,`expiryTime`,`updatedOn`)
                                VALUES ({key},{value},{time.time()+expiryTime},{time.time()})
                            """
                self.write_query(insertQuery)
                self.close()
                db_result = deque(db_result)
                db_result.appendleft({
                    "key": key,
                    "value": value,
                    "expiryTime": time.time()+expiryTime,
                })
                db_result = list(db_result)

                response.data.update({
                    "cachedValues": db_result
                })
            except Exception as error:
                response.handel_exception(logger,'set key values',error,db_err=False)
        except Exception as error:
            response.handel_exception(logger,'set key values',error,db_err=False)

        return response
