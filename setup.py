from distutils.core import setup
setup(name = 'dynamite',
      version ='1.0',
      description ='A Python Dynamo clone',
      author ='John Novatnack',
      author_email ='jnovatnack@gmail.com',
      package_data = {'dynamite.storage.persistence' : 
                      ['sql/*.sql']},
      packages = ['dynamite',
                  'dynamite.lib',
                  'dynamite.lib.consistent_hash',                  
                  'dynamite.load_balancer',
                  'dynamite.storage',
                  'dynamite.storage.datastore_view',
                  'dynamite.storage.persistence'],

      scripts = ['dynamite/load_balancer/load_balancer.py', 
                 'dynamite/storage/storage_node.py'])
