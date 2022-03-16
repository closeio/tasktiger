import os

# How much to delay scheduled tasks for testing purposes.
# Note that on macOS Catalina, when using an unsigned Python version, taskgated
# (com.apple.securityd) needs to approve launching the process. We therefore
# need ample time here (> 0.3s) in order to prevent test failures.
DELAY = 0.4

# Redis database number which will be wiped and used for the tests
TEST_DB = int(os.environ.get("REDIS_DB", 7))

# Redis hostname
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
