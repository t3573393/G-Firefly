#coding:utf8
'''
Created on 2013-5-8

@author: lan (www.9miao.com)
'''
from DBUtils.PooledDB import PooledDB, InvalidConnection
import pymysql

# DBCS = {'MySQLdb':"MySQLdb",}

# class DBPool(PooledDB):
#     """
#     """
#     def __init__(self, creator, *args, **kwargs):
#         PooledDB.__init__(self, creator, *args, **kwargs)
#         self.config = kwargs


class PyMysqlProxyDBConnection:
    """Proxy the pymysql connection api for the MySQLdb api"""

    def __init__(self, dbConnection):
        self._dbConnection = dbConnection

    def __getattr__(self, name):
        """Proxy all members of the class."""
        if self._dbConnection:
            return getattr(self._dbConnection, name)
        else:
            raise InvalidConnection

    def close(self):
        if self._dbConnection:
            self._dbConnection.close()

    def cursor(self, cursorclass=None):
        if cursorclass is None:
            self._dbConnection.cursor()
        return self._dbConnection.cursor(cursor=cursorclass)

    def __del__(self):
        """Delete the pooled connection."""
        try:
            self._dbConnection.close()
        except Exception:
            pass
        
class MultiDBPool(object):
    """
    """
    def __init__(self):
        """
        """
        self.router = None
    
    def initPool(self,config):
        """
        """
        self.dbpool = {}
        for dbkey,dbconfig in config.items():
            # _creator = DBCS.get(dbconfig.get('engine','MySQLdb'))
            # creator = __import__(_creator)
            # self.dbpool[dbkey] = PooledDB(creator=creator,**dbconfig)
            self.dbpool[dbkey] = PooledDB(creator=pymysql, **dbconfig)
            
    def bind_router(self,router):
        """
        """
        self.router = router()
        
    def getPool(self,write=True,**kw):
        """
        """
        if not self.router:
            return self.dbpool.values()[0]
        if write:
            dbkey = self.router.db_for_write(**kw)
            return self.dbpool[dbkey]
        else:
            dbkey = self.router.db_for_read(**kw)
            return self.dbpool[dbkey]
        
    def connection(self,write=True,**kw):
        """
        """
        if not self.router:
            return PyMysqlProxyDBConnection(self.dbpool.values()[0].connection(shareable=kw.get("shareable",True)))
        if write:
            dbkey = self.router.db_for_write(**kw)
            return PyMysqlProxyDBConnection(self.dbpool[dbkey].connection(shareable=kw.get("shareable",True)))
        else:
            dbkey = self.router.db_for_read(**kw)
            return PyMysqlProxyDBConnection(self.dbpool[dbkey].connection(shareable=kw.get("shareable",True)))


dbpool = MultiDBPool()

