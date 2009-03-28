#!/usr/bin/env python
# ------------------------------------------------------
# Imports
# ------------------------------------------------------
import logging
import xmlrpclib
import exceptions
from optparse import OptionParser
from SimpleXMLRPCServer import SimpleXMLRPCServer

from dynamite.storage.datastore_view import DataStoreView

# ------------------------------------------------------
# Config
# ------------------------------------------------------
logging.basicConfig(level=logging.INFO)

# ------------------------------------------------------
# Implementation
# ------------------------------------------------------
class LoadBalancer(object):
    """
    A load balancer that routes requests to the appropriate storage node
    """
    CONN_STR = 'http://%s'
    
    def __init__(self, servers, port):
        """
        Parameters:
            servers : list(str)
                A list of servers. Each server name is in the 
                format {host/ip}:port
        """
        self.port = int(port)
        self.server = None
        if not servers:
            raise exceptions.ValueError("Cannot have empty server list")
        
        # Create the load balancer's view of the storage node ring
        self.datastore_view = DataStoreView(servers)
        
        # Open connections to each server
        self.server_conns = {}
        for server in servers:
            self.server_conns[server] = xmlrpclib.ServerProxy(self.CONN_STR % server)

    # ------------------------------------------------------
    # Public methods
    # ------------------------------------------------------                
    def run(self):
        """
        Main storage node loop
        """
        self.server = SimpleXMLRPCServer(('', self.port), allow_none=True)
        self.server.register_function(self.get, "get")
        self.server.register_function(self.put, "put")  
        self.server.serve_forever()

    # ------------------------------------------------------
    # RPC methods
    # ------------------------------------------------------         
    def get(self, key):
        """
        Gets a key from the appropriate storage node
        
        :Parameters:
            key : str
                The key value        
        """
        value = None
        try:
            # Find the responsbile node
            respon_node = self.datastore_view.get_node(key)
            logging.debug('Getting key=%s from node=%s' % (key, respon_node))        
    
            # Get the value from that node
            value = self.server_conns[respon_node].get(key)
            
            logging.debug('Value=%s' % value)
        except:
            logging.error('Error getting the key=%s' % key)        
            value = None
        return value
    
    def put(self, key, value, context=None):
        """
        Puts a key in the appropriate datastore
        
        :Parameters:
            key : str
                The key name
            value : str
                The value
            context : str
                Should be only be None for now.  In the future an application will be
                able to add a custom context string
                
        :rtype: str
        :returns 200 if the operation succeeded, 400 otherwise
        """
        respon_code = None
        try:
            # Find the responsbile node
            respon_node = self.datastore_view.get_node(key)
            logging.debug('Putting key=%s on node=%s' % (key, respon_node))        

            # Get the value from that node
            respon_code = self.server_conns[respon_node].put(key, value)                
        except:
            logging.error("Error putting key=%s, value=%s" % (key,value))
            respon_code = "400"
        return respon_code

# ------------------------------------------------------
# Main
# ------------------------------------------------------
def parse_args():
    parser = OptionParser()
    parser.add_option('-s', '--server', dest='servers',
                      help='List of storage nodes(one per server) (Required)',
                      action='append', default=[])
    parser.add_option('-p', '--port', dest='port', default=30000,
                      help='Port to start the storage node on')    

    options, args = parser.parse_args()
    if not options.servers:
        parser.print_help()
        exit(-1)
        
    return options

if __name__ == '__main__':
    options = parse_args()
    load_balancer = LoadBalancer(options.servers, options.port)
    load_balancer.run()
