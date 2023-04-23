import sys

# Configuration file for the Sphinx documentation builder.

sys.path.append('/mobility')

# -- Project information

project = 'Mobility'
copyright = '2023, MIT Licence'
author = 'Mutliple authors'

release = '0.1'
version = '0.1'

# -- General configuration

extensions = [
    "myst_parser",
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
]
