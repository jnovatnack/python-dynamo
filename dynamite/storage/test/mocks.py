# --------------------------------------------
# Imports
# --------------------------------------------
from dynamite.storage.storage_node import StorageNode
from dynamite.storage.persistence.sqlite_persistence_layer import SqlitePersistenceLayer

# --------------------------------------------
# Mocking functions
# --------------------------------------------
def get_mock_storage_node():
    """
    Gets a storage node with an in-memory sqlite persistence layer
    
    :rtype: StorageNode
    :returns: A storage node with an in-memory sqlite persistence layer 
    """
    StorageNode._load_persistence_layer = lambda obj: None
    sn = StorageNode([], 1111111)
    sn.persis = SqlitePersistenceLayer('test1', ':memory:')
    sn.persis.init_persistence()
    
    return sn