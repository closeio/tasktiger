import os

# How much to delay scheduled tasks for testing purposes.
DELAY = 0.2

# How much time to wait until we assume a new TaskTiger process was started.
# Note that on macOS Catalina, when using an unsigned Python version,
# taskgated (com.apple.securityd) needs to approve launching the process.
# We therefore need ample here in order to prevent test failures.
PROCESS_DELAY = 2 * DELAY

# Redis database number which will be wiped and used for the tests
TEST_DB = int(os.environ.get('REDIS_DB', 7))

# Redis hostname
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
