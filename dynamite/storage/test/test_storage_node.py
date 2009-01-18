# --------------------------------------------
# Imports
# --------------------------------------------
import logging
from unittest import TestCase

from dynamite.storage.storage_node import StorageNode
from dynamite.storage.persistence.sqlite_persistence_layer import SqlitePersistenceLayer

# --------------------------------------------
# Config
# --------------------------------------------
logging.basicConfig(level=logging.ERROR)

# --------------------------------------------
# Tests
# --------------------------------------------
class TestStorageNode(TestCase):
    def setUp(self):
        StorageNode._load_persistence_layer = lambda obj: None
        self.sn = StorageNode([], 1111111)
        self.sn.persis = SqlitePersistenceLayer('test1', ':memory:')
        self.sn.persis.init_persistence()                       

    def test_simple_put(self):
        """
        Ensures we can write a value to the data store
        """
        result = self.sn.put("foo", "bar")
        self.assertEquals(result, '200')
        
    def test_simple_get(self):
        """
        Ensures we can get a value we wrote to our data store
        """
        result = self.sn.put("foo", "bar")
        self.assertEquals(result, '200')

        result = self.sn.get("foo")
        self.assertEquals(result, "bar")
        
    def test_get_date_reconcile(self):
        """
        Ensures multiple values with the same key are reconciled by date.
        """
        result = self.sn.put("foo", "bar")
        self.assertEquals(result, '200')
        result = self.sn.put("foo", "bar2")
        self.assertEquals(result, '200')
        result = self.sn.put("foo", "bar3")
        self.assertEquals(result, '200')

        result = self.sn.get("foo")
        self.assertEquals(result, "bar3")
        
