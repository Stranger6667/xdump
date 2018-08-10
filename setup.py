# coding: utf-8
import sys

from setuptools import setup

import xdump


with open('README.rst') as file:
    long_description = file.read()

install_requires = [
    'attrs<19',
    'psycopg2<2.8',
    'click<7',
]
if sys.version_info[0] == 2:
    install_requires.append('repoze.lru==0.7')

setup(
    name='xdump',
    url='https://github.com/Stranger6667/xdump',
    version=xdump.__version__,
    packages=['xdump'],
    license='MIT',
    author='Dmitry Dygalo',
    author_email='dadygalo@gmail.com',
    maintainer='Dmitry Dygalo',
    maintainer_email='dadygalo@gmail.com',
    keywords=['database', 'dump', 'postgresql'],
    description='Consistent partial database dump utility',
    long_description=long_description,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    include_package_data=True,
    install_requires=install_requires,
    extras_require={
        'django':  ['django>=1.11'],
    },
    entry_points='''
        [console_scripts]
        xdump=xdump.cli.dump:dump
        xload=xdump.cli.load:load
    '''
)
