# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
from importlib.metadata import PackageNotFoundError, version as pkg_version


def get_release():
    """Determine the borgstore version, without requiring borgstore to be installed."""
    try:
        return pkg_version("borgstore")
    except PackageNotFoundError:
        pass
    # not installed - maybe we are in a source tree where setuptools_scm already generated _version.py.
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
    try:
        from borgstore._version import version  # noqa

        return version
    except ImportError:
        # neither installed nor built - the docs do not really need the version, so just go on without it.
        return ""


project = "BorgStore"
copyright = "2026, Thomas Waldmann"
author = "Thomas Waldmann"
release = get_release()
version = ".".join(release.split(".")[:2])

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = []

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]
html_show_sphinx = False
