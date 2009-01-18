# -------------------------------------------------
# Imports
# -------------------------------------------------   
import exceptions
import uuid
from unittest import TestCase
from dynamite.lib.consistent_hash.consistent_hash import ConsistentHash
from collections import defaultdict

# -------------------------------------------------
# Tests
# -------------------------------------------------
class TestConsistentHash(TestCase):
    def test_simple_adding(self):
        """
        Ensures one can add a node to the consistent hash
        """
        cons_hash = ConsistentHash(2)
        cons_hash.add('192.168.1.1')        
        self.assertEquals(len(cons_hash), 2)
        
        cons_hash.add('192.168.1.2')        
        self.assertEquals(len(cons_hash), 4)        
        
        self.assertTrue(cons_hash._is_consistent())   
                
    def test_adding_same_node(self):
        """
        Ensures The hash throws an exception when the same name is
        added twice.
        """
        cons_hash = ConsistentHash(2)
        cons_hash.add('192.168.1.1')        

        threw_value_error = False
        try:
            cons_hash.add('192.168.1.1')
        except exceptions.ValueError:
            threw_value_error = True
        self.assertTrue(threw_value_error) 
        
        self.assertTrue(cons_hash._is_consistent())               
        
    def test_deleting(self):
        """
        Ensures that a node can be deleted
        """
        cons_hash = ConsistentHash(2)
        cons_hash.add('192.168.1.1')        
        self.assertEquals(len(cons_hash), 2)        
        cons_hash.delete('192.168.1.1')                
        self.assertEquals(len(cons_hash), 0)                
          
        self.assertTrue(cons_hash._is_consistent())  
        
    def test_get_empty_ring(self):
        """
        Ensures that an exception is thrown when the ring is empty 
        """             
        cons_hash = ConsistentHash(2)

        threw_value_error = False
        try:
            cons_hash.get_node('192.168.1.1')
        except exceptions.ValueError:
            threw_value_error = True
        self.assertTrue(threw_value_error)
        
    def test_getting_keys(self):
        """
        Ensures that random keys match to nodes
        """                 
        cons_hash = ConsistentHash(2)   
        
        nodes = ['192.168.1.1:20000',
                 '192.168.1.1:20001',
                 '192.168.1.1:20002',
                 '192.168.1.1:20003']                 

        for node in nodes:
            cons_hash.add(node)
            
        self.assertEquals(len(cons_hash), 8)
        node_counts = defaultdict(int)
        for i in xrange(0,100):
            key = str(uuid.uuid4())
            node = cons_hash.get_node(key)
            
            self.assertTrue(node in nodes)
            node_counts[node] += 1

        self.assertTrue(cons_hash._is_consistent()) 
            
