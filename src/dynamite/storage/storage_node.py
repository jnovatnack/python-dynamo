# ------------------------------------------------------
# Imports
# ------------------------------------------------------
import logging
import xmlrpclib
import SocketServer
import socket
from SimpleXMLRPCServer import SimpleXMLRPCServer
from optparse import OptionParser

from dynamite.storage.datastore_view import DataStoreView

# ------------------------------------------------------
# Config
# ------------------------------------------------------
logging.basicConfig(level=logging.DEBUG)

# A simple threaded xml rpc-server
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass

# ------------------------------------------------------
# Implementation
# ------------------------------------------------------
class StorageNode(object):
    """
    A storage node. 
    """
    GET = 'GET'
    PUT = 'PUT'
    
    def __init__(self, servers, port):
        """
        Parameters:
            servers : list(str)
                A list of servers.  Each server name is in the 
                format {host/ip}:port
            port : int
                Port number to start on
        """
        self.port = int(port)
        if servers is None:
            servers = []
        
        # Add myself to the servers list
        self.my_name = str(self)
        servers.append(self.my_name)
        self.datastore_view = DataStoreView(servers, self.my_name)

    def run(self):
        """
        Main storage node loop
        """
        self.server = AsyncXMLRPCServer(('', self.port), allow_none=True)
        self.server.register_function(self.get, "get")
        self.server.socket.settimeout(1.0)
        self.server.serve_forever()
        
    def get(self, key):
        """
        Gets a key
        
        :Parameters:
            key : str
                The key value
        """
        # Make sure I am supposed to have this key
        respon_node = self.datastore_view.get_node(key)
        if respon_node != self.my_name:
            logging.info("I'm not responsible for %s (%s vs %s)" % (key, 
                                                                    respon_node, 
                                                                    self.my_name))
            return None, None
        
        # Read it from the database
        
        # If the contexts don't line up then return both
        
        return 'value', 'foo'
    
    def put(self, key, value, context=None):
        """
        Puts a key value in the datastore
        
        :Parameters:
            key : str
                The key name
            value : str
                The value
            context : str
                Should be a date time stamp
        """
        # Make sure I am supposed to have this key
        if self.datastore_view.get_node(key) != self.my_name:
            logging.info("I'm not responsible for %s" % key)
            return None

        # Read it from the database
        
        # If the contexts don't line up then return both

        return 'OKAY'        
         
    def __str__(self):
        """
        Builds a string representation of the storage node
        
        :rtype: str
        :returns: A string representation of the storage node 
        """
        if getattr(self, 'port'):
            return '%s:%s' % (socket.gethostbyname(socket.gethostname()), self.port)
        else:
            return '%s' % socket.gethostbyname(socket.gethostname())
        

# ------------------------------------------------------
# Main
# ------------------------------------------------------
def parse_args():
    parser = OptionParser()
    parser.add_option('-s', '--server', dest='servers',
                      help='List of storage nodes(one per server)',
                      action='append', default=[])
    parser.add_option('-p', '--port', dest='port', default=25000,
                      help='Port to start the storage node on')

    options, args = parser.parse_args()
    return options

if __name__ == '__main__':
    options = parse_args()
    storage_node = StorageNode(options.servers, options.port)
    storage_node.run()


