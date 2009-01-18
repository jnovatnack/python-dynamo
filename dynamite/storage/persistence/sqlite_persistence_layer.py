# ------------------------------------------------------
# Imports
# ------------------------------------------------------
import logging
import sqlite3
import os
import datetime

from dynamite.storage.persistence.persistence_layer import PersistenceLayer

# ------------------------------------------------------
# Implementation
# ------------------------------------------------------
class SqlitePersistenceLayer(PersistenceLayer):
    """
    A rudimentary sqlite persistence layer
    """
    SQL_FILE = 'sql/sqlite.sql'
    
    def __init__(self, name, conn_str=None):
        """
        :Parameters:
            name : Name of the server
        """        
        self.name = name
        if not conn_str:
            self.conn_str = '/tmp/%s' % self.name
        else:
            self.conn_str = conn_str
        self.conn = None
        
    def __del__(self):
        """
        Destructor closes sqlite connection
        """
        self.close()

    def init_persistence(self):
        """
        Initializes the persistence layer.
        """
        logging.info('Conncting to sqlite db %s' % self.conn_str)
        self.conn = sqlite3.connect(self.conn_str)

        rows = [row for row in self.conn.execute("SELECT * FROM key_values")]
        try:
            f = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                  self.SQL_FILE))
            command = []
            for line in f:
                line = line.strip()
                command.append(line)
                
                if line.endswith(';'):
                    command = ' '.join(command)
                    self.conn.execute(command)
                    command = []
                    
            self.conn.commit()
        except:
            logging.error('Error loading %s' % self.SQL_FILE)
        
    def close(self):
        """
        Closes the db connection
        """
        if self.conn:
            self.conn.close()
            
    def get_key(self, key):
        """
        Reads a key from the db.
        
        :Parameters:
            key : str      
        :rtype: list(tuple)
        :returns A list of (id, blob, datetime) tuples for the key
        """
        if not self.conn:
            logging.info('SQLite connection not open')
            return []
        
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT id,value,date FROM key_values WHERE key=?", (key,))
            result = [row for row in cur]
        except:
            logging.error('Error getting key=%s' % key)
            raise
            result = []
            
        return result
    
    def put_key(self, key, data):
        """
        Puts a key in the database
        
        :Parameters:
            key : str
                The key name
            data : str
                The data string
        """
        if not self.conn:
            logging.info('SQLite connection not open')

        try:
            now = datetime.datetime.now()
            cur = self.conn.cursor()
            cur.execute("INSERT INTO key_values(key, value, date) VALUES (?, ?, ?)",
                        (key, data, now))
            self.conn.commit()
            result = True
        except:
            logging.error('Error putting key=%s, data=%s' % (key, data))
            result = False
            
        return result
        
        
    