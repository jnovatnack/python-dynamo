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
    