Development
===========

Making a release
----------------

Update ``docs/changes.rst`` (the heading of the new section belongs onto the
last commit that goes into the release) and merge that via a pull request.
Then put an annotated, signed tag named like the version (no ``v`` prefix) onto
the "update CHANGES" commit and push it::

    git tag -s -m "tagged/signed release 0.7.0" 0.7.0
    git push origin 0.7.0

Pushing the tag runs ``.github/workflows/release.yml``, which builds the sdist,
checks that it is complete and installable, and creates a *draft* GitHub
release with it. The upload to PyPI happens in the ``pypi`` job, which uses
trusted publishing (no API token) and waits for an approval if the ``pypi``
environment has required reviewers configured.

Finally, write the release notes and publish the draft release.
