# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'ForecrypT'
copyright = '2024, dove0dowg'
author = 'dove0dowg'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon'  # Google-style
]

extensions.append("sphinx_wagtail_theme")
html_theme = 'sphinx_wagtail_theme'

# These are options specifically for the Wagtail Theme.
html_theme_options = dict(
    project_name = "ForecrypT",
    logo = "img/wagtail-logo-circle.svg",
    logo_alt = "Wagtail",
    logo_height = 59,
    logo_url = "/",
    logo_width = 45,
    github_url = "https://github.com/dove0dowg/forecrypt/tree/master/docs/",
    source_link = "https://github.com/dove0dowg/forecrypt",
    footer_links = ""
)

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'utils/**', 'docstring_export.py', 'forecrypt_api.py']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output




html_static_path = ['_static']
html_logo = ''
# -- path --

import os
import sys
sys.path.insert(0, os.path.abspath('../'))  # path