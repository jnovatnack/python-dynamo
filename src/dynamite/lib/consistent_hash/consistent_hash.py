# -------------------------------------------------
# Imports
# -------------------------------------------------    
import md5
import exceptions
import logging

# -------------------------------------------------
# Config
# -------------------------------------------------    
logging.basicConfig()

# -------------------------------------------------
# Consistent Hash
# -------------------------------------------------    
class ConsistentHash(object):
    """
    A consistent hash.  
    
    Adapted from:
    http://amix.dk/blog/viewEntry/19367
    
    Also referenced:
    http://www.lexemetech.com/2007/11/consistent-hashing.html
    
    :Parameters:
        replication : int
            Number of times a node is replicated
    """
    REPLICATION_STR = '%s-%s'
    
    def __init__(self, replication=2):
        """
        :Parameters:
            replication : int
                Replication per node
        """
        self.replication_factor = replication
        self.ring = dict()
        self.sorted_keys = []
    
    # -------------------------------------------------
    # Public methods
    # -------------------------------------------------
    def add(self, node):
        """
        Adds a node to the hash.  Note that the node
        must support str()
        
        :Parameters:
            node : object
                Any object that you wish to add to the hash
        """
        for i in xrange(0, self.replication_factor):
            hash_key = self._gen_key(str(self.REPLICATION_STR % (node, i)))
            if not hash_key in self.ring:
                self.ring[hash_key] = node
            else:
                raise exceptions.ValueError('Key %s already exists' % hash_key)

            # Add the key to the sorted list.  If the position is 0 must 
            # disambiguate between this being the largest and smallest key            
            pos = self._get_pos(hash_key)
            if pos == 0 and self.sorted_keys and hash_key > self.sorted_keys[-1]:
                self.sorted_keys.append(hash_key)
            else:
                self.sorted_keys.insert(pos, hash_key)
    
    def delete(self, node):
        """
        Removes a node from the hash
        
        :Parameters:
            obj : object
                Any object that you wish to delete from the hash
        """
        for i in xrange(0, self.replication_factor):
            hash_key = self._gen_key(str(self.REPLICATION_STR % (node, i)))
            if hash_key in self.ring:
                del self.ring[hash_key]
                self.sorted_keys.remove(hash_key)
            else:
                logging.info('%s not found in the consistent hash' % str(obj))
    
    def get_node(self, key):
        """
        Gets the virtual node that the key maps to
        
        :Parameters:
            key : str
                The key name
        :rtype: object
        :returns: The node corresponding to the key
        """
        if len(self.sorted_keys) == 0:
            raise exceptions.ValueError('ring is empty cannot get %s' % key)
        
        # Find the first key greater than the key of the input string
        hash_key = self._gen_key(key)
        pos = self._get_pos(hash_key)
        
        return self.ring[self.sorted_keys[pos]]
    
    def __len__(self):
        """
        Returns the number of virtual nodes in the hash
        
        :rtype: int
        :returns: The number of virtual nodes in the hash
        """
        return len(self.ring)
    
    # -------------------------------------------------
    # Protected methods
    # -------------------------------------------------    
    def _get_pos(self, hash_key):
        """
        Gets the position of a ring in the consistent hash
        """
        pos = None
        # Find the first key greater than the key of the input string        
        for ring_pos, pos_val in enumerate(self.sorted_keys):
            if pos_val >= hash_key:
                pos = ring_pos
                break

        # If nothing is greater than loop around and go with the first            
        if pos is None:
            pos = 0
        return pos
            
    def _gen_key(self, key):
        """
        Given a key returns its long using the md5 hash
        
        :Parameters:
            key : str
                A key
        :rtype: long
        :returns: A long
        """
        m = md5.new()
        m.update(key)
        return long(m.hexdigest(), 16)
    
    def _is_consistent(self):
        """
        Ensures that the sorted list of keys and hash keys are in
        agreement
        """
        len_consistency = len(self.ring) == len(self.sorted_keys)
        hash_keys = self.ring.keys()
        hash_keys.sort()
        key_consistency = hash_keys == self.sorted_keys

        return len_consistency and key_consistency
