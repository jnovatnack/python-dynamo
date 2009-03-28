from distutils.core import setup
setup(name = 'dynamo',
      version ='1.0',
      description ='A Python Dynamo clone',
      author ='John Novatnack',
      author_email ='jnovatnack@gmail.com',
      package_data = {'dynamo.storage.persistence' : 
                      ['sql/*.sql']},
      packages = ['dynamo',
                  'dynamo.lib',
                  'dynamo.lib.consistent_hash',                  
                  'dynamo.load_balancer',
                  'dynamo.storage',
                  'dynamo.storage.datastore_view',
                  'dynamo.storage.persistence'],

      scripts = ['dynamo/load_balancer/load_balancer.py', 
                 'dynamo/storage/storage_node.py'])
