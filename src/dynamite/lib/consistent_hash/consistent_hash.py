# -------------------------------------------------
# Imports
# -------------------------------------------------    
import md5
import exceptions
import logging
import random
from collections import defaultdict

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
    
    Adapted from: http://amix.dk/blog/viewEntry/19367
    
    :Parameters:
        replication : int
            Number of times a node is replicated
    """
    REPLICATION_STR = '%s-%s'
    DETERMINISTIC = 'deterministic'
    STRATEGY1 = 'strategy1'
    
    def __init__(self, replication=2, strategy=DETERMINISTIC):
        """
        :Parameters:
            replication : int
                Number of virtual instances per node
            strategy : str
                The strategy for mapping nodes to hash values
        """
        self.replication_factor = replication
        self.ring = dict()
        self.sorted_keys = []
        self.node_tokens = defaultdict(list)
        self.strategy = strategy

    def __len__(self):
        """
        Returns the number of virtual nodes in the hash
        
        :rtype: int
        :returns: The number of virtual nodes in the hash
        """
        return len(self.ring)
    
    # -------------------------------------------------
    # Public methods
    # -------------------------------------------------
    def add(self, node, strategy='deterministic'):
        """
        Adds a node to the hash.  
        
        Strategies:
            DETERMINISTC : token is created from the node name.  Note that this can
                           result in non-uniform node distributions, but is necessary 
                           with messaging between storage nodes.
            STRATEGY1    : For each node self.replication_factor ftokens are added 
                           to the hash with the keys randomly chosen from the hash 
                           space.  This represents partition strategy 1 from
                           "Dynamo : amazons highly available key-value store"
        
        Note that the node must support str().  
        
        :Parameters:
            node : object
                Any object that you wish to add to the hash
        """
        if self.node_tokens.get(node):
            raise exceptions.ValueError('Node %s already in the consistent hash' % node) 
        
        for i in xrange(0, self.replication_factor):
            hash_key = self._get_node_hash_key(node, i)
            self.node_tokens[node].append(hash_key)
            self.ring[hash_key] = node

            # Add the key to the sorted list.  If the position is 0 must 
            # disambiguate between this being the largest and smallest key            
            pos = self._get_pos(hash_key)
            if pos == 0 and self.sorted_keys and hash_key > self.sorted_keys[-1]:
                self.sorted_keys.append(hash_key)
            else:
                self.sorted_keys.insert(pos, hash_key)
        
    def remove(self, node):
        """
        Removes a node from the hash
        
        :Parameters:
            obj : object
                Any object that you wish to delete from the hash
        """
        for token in self.node_tokens.get(node, []):
            if token in self.ring:
                del self.ring[token]
                self.sorted_keys.remove(token)
            else:
                logging.info('%s not found in the consistent hash' % str(obj))
        del self.node_tokens[node]
         
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
        
    # -------------------------------------------------
    # Protected methods
    # -------------------------------------------------    
    def _get_pos(self, hash_key):
        """
        Gets the position of a ring in the consistent hash
        
        :Parameters:
            key : long
                The hash key
        :rtype: int
        :returns: The position of the key in the hash ring  
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
        
        :rtype: bool
        :returns: True if the ring and sorted keys are consistent
        """
        len_consistency = len(self.ring) == len(self.sorted_keys)
        hash_keys = self.ring.keys()
        hash_keys.sort()
        key_consistency = hash_keys == self.sorted_keys

        return len_consistency and key_consistency
    
    def _get_node_hash_key(self, node, rep_num):
        """
        Gets the hash key for a node and replication number given the
        partitioning strategy
        
        :Parameters:
            node : object
                A node
            rep_num : int
                The replication number
        """
        if self.strategy == self.STRATEGY1:
            hash_key = random.randint(0, 2**128)
        else:             
            hash_key = self.REPLICATION_STR % (node, rep_num)
        return hash_key           
