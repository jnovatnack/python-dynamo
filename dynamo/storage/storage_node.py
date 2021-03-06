#!/usr/bin/env python
# ------------------------------------------------------
# Imports
# ------------------------------------------------------
import logging
import xmlrpclib
import socket
from SimpleXMLRPCServer import SimpleXMLRPCServer
from optparse import OptionParser
from datetime import datetime, timedelta

from dynamo.storage.datastore_view import DataStoreView
from dynamo.storage.persistence.sqlite_persistence_layer import SqlitePersistenceLayer

# ------------------------------------------------------
# Config
# ------------------------------------------------------
logging.basicConfig(level=logging.INFO)

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
        self.server = None
        if servers is None:
            servers = []
        
        # Add myself to the servers list
        self.my_name = str(self)
        servers.append(self.my_name)
        self.datastore_view = DataStoreView(servers)

        # Load the persistence layer
        self._load_persistence_layer()
             
    def __del__(self):
        """
        Destructor
        """
        if self.persis:
            self.persis.close()    
        if self.server:
            self.server.server_close()
        
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
        Gets a key
        
        :Parameters:
            key : str
                The key value
        """
        logging.debug('Getting key=%s' % key)
        # Make sure I am supposed to have this key
        respon_node = self.datastore_view.get_node(key)
        if respon_node != self.my_name:
            logging.info("I'm not responsible for %s (%s vs %s)" % (key, 
                                                                    respon_node, 
                                                                    self.my_name))
            return None
        
        # Read it from the database
        result = self.persis.get_key(key)
        
        # If the contexts don't line up then return the most recent
        value = None
        if len(result) == 1:
            value = result[0][1]
        else:
            value = self._reconcile_conflict(result)[0]

        logging.debug('Returning value=%s' % value)        
        return value
    
    def put(self, key, value, context=None):
        """
        Puts a key value in the datastore
        
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
        # Make sure I am supposed to have this key
        if self.datastore_view.get_node(key) != self.my_name:
            logging.info("I'm not responsible for %s" % key)
            return None
        
        res_code = None
        try:
            # Read it from the database
            result = self.persis.put_key(key, value)
            res_code = '200'
        except:
            logging.error('Error putting key=%s value=%s into the persistence layer' % 
                          (key, value))
            res_code = '400'
        
        return res_code
         
    # ------------------------------------------------------
    # Private methods
    # ------------------------------------------------------  
    def _reconcile_conflict(self, result):
        """
        Reconciles the conflict between a number of values.  Note
        that currently this defaults to taking the last written value.
        In the future this will be expanded to allow application specific
        conflict resolution
        
        :Parameters:
            result : list(tuples)
                A list of result tuples from the persistence layer in the form
                [(id, "value", "date"), ...]
        :rtype: tuple(int, str)
        :returns An id, string tuple of the chosen version
        """
        last_result = None
        last_date = None
        for res in result:
            if last_result is None:
                last_result = res[1]
                last_date = self._parse_date(res[2])  
            else:
                date = self._parse_date(res[2])
                if date > last_date:
                    last_date = date
                    last_result = res[1]
 
        return (last_result, last_date)

    def _load_persistence_layer(self):
        """
        Loads the persistence layer
        """
        # Setup my persistence layer
        self.persis = SqlitePersistenceLayer(self.my_name)
        self.persis.init_persistence()  
        
    def _parse_date(self, datestr):
        """
        Parses an iso formatted date
        
        :Parameters:
            datestr : str
                An iso formatted date
        :rtype: datetime
        :returns A date object
        """
        date_str, micros = datestr.split('.')
        date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        date += timedelta(microseconds=float(micros))
        
        return date
        
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
