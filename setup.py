# workaround for open() with encoding='' python2/3 compability
from io import open
from setuptools import setup
import sys

with open('README.rst', encoding='utf-8') as file:
    long_description = file.read()

install_requires = [
    'click',
    'redis>=2,<3',
    'structlog'
]

if sys.version_info < (3, 3):
    install_requires += ['contextlib2>=0.5.5']

tests_require = install_requires + [
    'freezefrog',
    'pytest',
    'psutil',
]

setup(
    name='tasktiger',
    version='0.9.5',
    url='http://github.com/closeio/tasktiger',
    license='MIT',
    description='Python task queue',
    long_description=long_description,
    test_suite='tests',
    tests_require=tests_require,
    platforms='any',
    install_requires=install_requires,
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    packages=[
        'tasktiger',
    ],
    package_data={'tasktiger': ['lua/*.lua']},
    entry_points={
        'console_scripts': [
            'tasktiger = tasktiger:run_worker',
        ],
    },
)
