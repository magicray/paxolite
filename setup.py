import time
from distutils.core import setup


setup(version=time.strftime('%y%m%d.%H%M%S'),
      name='paxolite',
      package=['paxolite'],
      description='Replicated Key Value store, '
                  'using Paxos for replication and SQLite for storage',
      author='Bhupendra Singh',
      author_email='bhsingh@gmail.com',
      url='https://github.com/magicray/paxolite')

# python3 setup.py sdist upload
