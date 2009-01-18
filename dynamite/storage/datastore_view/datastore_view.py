# ------------------------------------------------------
# Imports
# ------------------------------------------------------
import logging

from dynamite.lib.consistent_hash import ConsistentHash

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
               
    def get_node(self, key):
        """
        Gets the node responsible for a particular key
        
        :Parameters:
            key : str
                The key
        :rtype: str
        :returns: The node name responsible for the key
        """
        node = self.consistent_hash.get_node(key)
        return node
    
