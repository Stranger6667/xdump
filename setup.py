# coding: utf-8
from setuptools import setup

import concopy


with open('README.rst') as file:
    long_description = file.read()


setup(
    name='concopy',
    url='https://github.com/Stranger6667/concopy',
    version=concopy.__version__,
    packages=['concopy'],
    license='MIT',
    author='Dmitry Dygalo',
    author_email='dadygalo@gmail.com',
    maintainer='Dmitry Dygalo',
    maintainer_email='dadygalo@gmail.com',
    keywords=['database', 'copy', 'postgresql'],
    description='Consistent partial database copy utility',
    long_description=long_description,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    include_package_data=True,
    install_requires=['attrs', 'cached_property', 'psycopg2'],
)
