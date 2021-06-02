# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import os
readmefile = os.path.join(os.path.dirname(__file__), "README.md")
with open(readmefile) as f:
    readme = f.read()

setup(
    name='sqljob',
    version='0.1.0',
    description='Run long sql query in the background',
    author='Kota Mori', 
    author_email='kmori05@gmail.com',
    url='https://github.com/kota7/sqljob',
    download_url='https://github.com/kota7/sqljob/archive',
    long_description=readme,
    long_description_content_type="text/markdown"

    packages=['sqljob'],
    install_requires=['pandas', 'sqlalchemy'],
    test_require=[],
    package_data={},
    entry_points={},
    
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for

        # Pick your license as you wish (should match "license" above)
         'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        #'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        #'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8'
        'Programming Language :: Python :: 3.9'
    ],
    test_suite='tests'
)