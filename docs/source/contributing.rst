============================
Contributing to intake-xarray
============================

Contributions are highly welcomed and appreciated.  Every little help counts,
so do not hesitate!

.. contents:: Contribution links
   :depth: 2


.. _submitfeedback:

Feature requests and feedback
-----------------------------

Do you like intake-xarray?  Share some love on Twitter or in your blog posts!

We'd also like to hear about your propositions and suggestions.  Feel free to
`submit them as issues <https://github.com/intake/intake-xarray>`_ and:

* Explain in detail how they should work.
* Keep the scope as narrow as possible.  This will make it easier to implement.


.. _reportbugs:

Report bugs
-----------

Report bugs for intake-stac in the `issue tracker <https://github.com/intake/intake-xarray>`_.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting,
  specifically the Python interpreter version, installed libraries, and intake-stac
  version.
* Detailed steps to reproduce the bug.

If you can write a demonstration test that currently fails but should pass
(xfail), that is a very useful commit to make as well, even if you cannot
fix the bug itself.


.. _fixbugs:

Fix bugs
--------

Look through the `GitHub issues for bugs <https://github.com/intake/intake-xarray/labels/type:%20bug>`_.

Talk to developers to find out how you can fix specific bugs.


Write documentation
-------------------

intake-xarray could always use more documentation.  What exactly is needed?

* More complementary documentation.  Have you perhaps found something unclear?
* Docstrings.  There can never be too many of them.
* Blog posts, articles and such -- they're all very appreciated.

You can also edit documentation files directly in the GitHub web interface,
without using a local copy.  This can be convenient for small fixes.

.. note::
    Build the documentation locally with the following command:

    .. code:: bash

        $ conda env create -f docs/environment.yml
        $ cd docs
        $ make html

    The built documentation should be available in the ``docs/_build/``.



 .. _`pull requests`:
.. _pull-requests:


Preparing Pull Requests
-----------------------


#. Fork the
   `intake-xarray GitHub repository <https://github.com/intake/intake-xarray>`__.  It's
   fine to use ``intake-xarray`` as your fork repository name because it will live
   under your user.


#. Clone your fork locally using `git <https://git-scm.com/>`_ and create a branch::

    $ git clone git@github.com:YOUR_GITHUB_USERNAME/intake-xarray.git
    $ cd intake-xarray

    # now, to fix a bug or add feature create your own branch off "master":

    $ git checkout -b your-bugfix-feature-branch-name master


#. Install development version in a conda environment::

    $ conda env create -f ci/environment-py39.yml
    $ conda activate test_env
    $ pip install . -e


#. Run all the tests

   Now running tests is as simple as issuing this command::

    $ pytest --verbose


   This command will run tests via the "pytest" tool


#. Commit and push once your tests pass and you are happy with your change(s)::

    $ git commit -a -m "<commit message>"
    $ git push -u


#. Finally, submit a pull request through the GitHub website using this data::

    head-fork: YOUR_GITHUB_USERNAME/intake-xarray
    compare: your-branch-name

    base-fork: intake/intake-xarray
    base: master


 .. _`release a new version`:
.. _release-a-new-version:


Release a new version
---------------------

intake-xarray uses the pypipublish GitHub action to publish new versions on PYPI. Just create a new tag `git tag 0.4.1`, `git push upstream --tags`, then create a release by visiting https://github.com/intake/intake-xarray/releases/new. When the release is created the version will automatically be uploaded to https://pypi.org/project/intake-xarray/. 

