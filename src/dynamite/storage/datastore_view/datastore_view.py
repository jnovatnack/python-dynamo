# ------------------------------------------------------
# Imports
# ------------------------------------------------------
import logging

from dynamite.lib.consistent_hash import ConsistentHash
from dynamite.storage.persistence.sqlite_persistence_layer import SqlitePersistenceLayer

# ------------------------------------------------------
# Implementation
# ------------------------------------------------------
class DataStoreView(object):
    """
    A storage node/load balancers view of the storage nodes.
    """
    def __init__(self, servers, name):
        """
        Parameters:
            servers : list(str)
                A list of servers.  Each server name is in the 
                format {host/ip}:port
            name : str
                The name of the node
        """
        self.consistent_hash = ConsistentHash()
        
        logging.info('Adding servers %s' % servers)
        for server in servers:
            self.consistent_hash.add(server)
            
        self.persis = SqlitePersistenceLayer(name)
        self.persis.init_persistence()

