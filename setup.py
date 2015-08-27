from setuptools import setup

setup(
    name='tasktiger',
    version='0.1',
    url='http://github.com/closeio/tasktiger',
    license='MIT',
    description='Python task queue',
    test_suite='tests',
    tests_require=['redis'],
    platforms='any',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
    ],
    packages=[
        'tasktiger',
    ],
    entry_points={
        'console_scripts': [
            'tasktiger = tasktiger:run_worker',
        ],
    },
)
