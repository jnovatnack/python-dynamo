# ------------------------------------------------------
# Imports
# ------------------------------------------------------
import logging
import sqlite3
import os

from dynamite.storage.persistence.persistence_layer import PersistenceLayer

# ------------------------------------------------------
# Implementation
# ------------------------------------------------------
class SqlitePersistenceLayer(PersistenceLayer):
    """
    A rudimentary sqlite persistence layer
    """
    SQL_FILE = 'sql/sqlite.sql'
    
    def __init__(self, name):
        """
        :Parameters:
            name : Name of the server
        """        
        self.name = name
        self.conn = None

    def init_persistence(self):
        """
        Initializes the persistence layer.
        """
        self.conn = sqlite3.connect('/tmp/%s' % self.name)
        try:
            f = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                  self.SQL_FILE))
            for line in f:
                self.conn.execute(line)
            self.conn.commit()
        except:
            logging.error('Error loading %s' % self.SQL_FILE)
            raise
    