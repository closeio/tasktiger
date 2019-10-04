import os

# How much to delay scheduled tasks for testing purposes.
DELAY = 0.2

# Redis database number which will be wiped and used for the tests
TEST_DB = int(os.environ.get('REDIS_DB', 7))

# Redis hostname
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
