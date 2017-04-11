from setuptools import setup

install_requires = [
    'click',
    'redis',
    'structlog'
]

tests_require = install_requires + [
    'pytest',
    'psutil',
]

setup(
    name='tasktiger',
    version='0.8.5',
    url='http://github.com/closeio/tasktiger',
    license='MIT',
    description='Python task queue',
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
