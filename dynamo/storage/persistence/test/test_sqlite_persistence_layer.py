# --------------------------------------------
# Imports
# --------------------------------------------
from unittest import TestCase

from dynamo.storage.persistence.sqlite_persistence_layer import SqlitePersistenceLayer

# --------------------------------------------
# Tests
# --------------------------------------------
class TestSqlitePersistenceLayer(TestCase):
    """
    Tests the sqlite persistence layer
    """
    def setUp(self):
        self.persis = SqlitePersistenceLayer('test_layer', ':memory:')
        self.persis.init_persistence()
        
    def tearDown(self):
        self.persis.conn.close()
        
    def test_simple_put(self):
        """
        Tests a put
        """
        self.persis.put_key('foo', 'this is my data')

        result = self.persis.conn.execute("SELECT * FROM key_values")
        rows = [row for row in result]
        self.assertEquals(len(rows), 1)
        
        row = rows[0][0:3]
        expected_values = (1, 'foo', 'this is my data')
        self.assertEquals(row, expected_values)
        
    def test_simple_get(self):
        """
        Tests a get
        """
        self.persis.put_key('foo', 'this is my data')
        result = self.persis.get_key('foo')
        self.assertEquals(len(result), 1)        
        self.assertEquals(result[0][0:2], (1, 'this is my data'))
        
    def test_put_multiple_values_per_key(self):
        """
        Ensures that we can put multiple values for the same key in the
        persistence layer.
        """
        self.persis.put_key('foo', 'this is my data')
        self.persis.put_key('foo', 'this is my data #2')        

        result = self.persis.conn.execute("SELECT * FROM key_values")
        rows = [row for row in result]
        self.assertEquals(len(rows), 2)

    def test_get_multiple_values_per_key(self):
        """
        Ensures that we can get multiple values for the same key in the
        persistence layer.
        """
        self.persis.put_key('foo', 'this is my data')
        self.persis.put_key('foo', 'this is my data #2')        

        result = self.persis.get_key('foo')
        self.assertEquals(len(result), 2)
        
        expected_rows = [(1, 'this is my data'),
                         (2, 'this is my data #2')]

        for row in result:
            row = row[0:2]           
            self.assertTrue(row in expected_rows)
            expected_rows.remove(row)
            

