# ------------------------------------------------------
# Imports
# ------------------------------------------------------
import logging
import xmlrpclib
import SocketServer
from socket import gethostname
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
        self.datastore_view = DataStoreView(servers, str(self))

    def get(self, key):
        """
        Gets a key
        
        :Parameters:
            key : str
                The key value
        """
        return 'value'
    
    def put(self, key, value):
        """
        Puts a key value in the datastore
        
        :Parameters:
            key : str
                The key name
            value : str
                The value
        """
        return 'OKAY'

    def run(self):
        """
        Main storage node loop
        """
        self.server = AsyncXMLRPCServer(('', self.port))
        self.server.register_function(self.get, "get")
        self.server.socket.settimeout(1.0)
        self.server.serve_forever()
         
    def __str__(self):
        """
        Builds a string representation of the storage node
        
        :rtype: str
        :returns: A string representation of the storage node 
        """
        if getattr(self, 'port'):
            return '%s:%s' % (gethostname(), self.port)
        else:
            return '%s' % gethostname()
        

# ------------------------------------------------------
# Main
# ------------------------------------------------------
def parse_args():
    parser = OptionParser()
    parser.add_option('-s', '--server', dest='servers',
                      help='List of storage nodes(one per server)',
                      action='append')
    parser.add_option('-p', '--port', dest='port', default=25000,
                      help='Port to start the storage node on')

    options, args = parser.parse_args()
    if not options.servers:
        parser.print_help()
        exit(1)

    return options

if __name__ == '__main__':
    options = parse_args()
    storage_node = StorageNode(options.servers, options.port)
    storage_node.run()


