import re

from setuptools import setup

VERSION_FILE = "tasktiger/__init__.py"
with open(VERSION_FILE, encoding="utf8") as fd:
    version = re.search(r'__version__ = ([\'"])(.*?)\1', fd.read()).group(2)

with open("README.rst", encoding="utf-8") as file:
    long_description = file.read()

install_requires = ["click", "redis>=3.3.0,<5", "structlog"]

tests_require = install_requires + ["freezefrog", "pytest", "psutil"]

setup(
    name="tasktiger",
    version=version,
    url="http://github.com/closeio/tasktiger",
    license="MIT",
    description="Python task queue",
    long_description=long_description,
    test_suite="tests",
    tests_require=tests_require,
    install_requires=install_requires,
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    packages=["tasktiger"],
    package_data={"tasktiger": ["lua/*.lua"]},
    entry_points={"console_scripts": ["tasktiger = tasktiger:run_worker"]},
)
