#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""seamm_jobserver
The JobServer for the SEAMM environment.
"""

import sys
from setuptools import setup, find_packages
import versioneer

short_description = __doc__.split("\n")

# from https://github.com/pytest-dev/pytest-runner#conditional-requirement
needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

with open('requirements.txt') as fd:
    requirements = fd.read()

setup(
    # Descriptive entries which should always be present
    name='seamm_jobserver',
    author="Paul Saxe",
    author_email='psaxe@molssi.org',
    description=short_description[1],
    long_description=readme + '\n\n' + history,
    long_description_content_type='text/markdown',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    license='LGPLv3+',
    url='https://github.com/molssi-seam/seamm_jobserver',

    # Which Python importable modules should be included when your package is
    # installed, handled automatically by setuptools. Use 'exclude' to prevent
    # some specific subpackage(s) from being added, if needed
    python_requires='>=3.12',
    packages=find_packages(include=['seamm_jobserver']),

    # Optional include package data to ship with your package. Customize
    # MANIFEST.in if the general case does not suit your needs. Comment out
    # this line to prevent the files from being packaged with your software
    include_package_data=True,

    # Allows `setup.py test` to work correctly with pytest
    setup_requires=[] + pytest_runner,

    # Required packages, pulls from pip if needed; do not use for Conda
    # deployment
    install_requires=requirements,

    test_suite='tests',

    # Valid platforms your code works on, adjust to your flavor
    platforms=['Linux',
               'Mac OS-X',
               'Unix',
               'Windows'],

    # Manual control if final package is compressible or not, set False to
    # prevent the .egg from being made
    # zip_safe=False,

    keywords='seamm_jobserver',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        ('License :: OSI Approved :: GNU Lesser General Public License v3 or '
         'later (LGPLv3+)'),
        'Natural Language :: English',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.12',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Chemistry',
    ],
    entry_points={
        'console_scripts': [
            'jobserver=seamm_jobserver.jobserver:run',
            'seamm-jobserver=seamm_jobserver.jobserver:run'
        ],
    }
)
