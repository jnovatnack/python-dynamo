# ------------------------------------------------------
# Imports
# ------------------------------------------------------
from exceptions import NotImplementedError

# ------------------------------------------------------
# Implementation
# ------------------------------------------------------
class PersistenceLayer(object):
    """
    An abstract persistence layer class
    """
    def init_persistence(self):
        """
        Initializes the persistence layer
        """
        raise NotImplementedError('init_persistence must be implemented')

    def close(self):
        """
        Closes the connection to the persistence layer
        """
        raise NotImplementedError('close must be implemented')
    
    def get_key(self, key):
        """
        Reads a key from the db.
        
        :Parameters:
            key : str      
        :rtype: list(tuple)
        :returns A list of (blob, datetime) tuples for the key
        """
        raise NotImplementedError('get_key must be implemented')            