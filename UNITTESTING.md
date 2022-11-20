
# Unit testing in Lazy Librarian

LazyLibrarian is introducing unit testing to make it more feasible to make changes to the project without causing major issues.  This is beginning in November 2022.

## Packages
The following packages are needed:
* unittest for Python
`sudo apt-get install python-setuptools`

I recommend getting these as well:
 * pytest module (to make it easier to run the tests)
 `pip3 install pytest`
 * pytest-cov (to get code coverage information)
`pip3 install pytest-cov`

## Execution

Run these from the Lazylibrarian root directory, where LazyLibrarian.py resides.

Using the unittest module:
`python -m unittest discover unittests/`

Or, using pytest:
`pytest unittests`

To also get code coverage information in readable form:
`pytest --cov=lazylibrarian unittests`

And to get code coverage in XML form to use in Visual Studio:
`pytest --cov-report=xml:cov.xml --cov=lazylibrarian unittests`

### Troubleshooting pytest
It's possible that pytest won't run, which might be because the file `pyproject.toml` can't be found. All it does is to add "." to the list of directories pytest searches for files in - it doesn't do that by default.

### Interpreting pytest results
Running pytest, the output should look something like this:

    ======================== test session starts ========================
    platform win32 -- Python 3.11.0, pytest-7.2.0, pluggy-1.0.0
    rootdir: P:\projects\LazyLibrarian, configfile: pyproject.toml
    plugins: cov-4.0.0
    collected 59 items
    
    unittests\test_formatter.py ...............................    [ 52%]
    unittests\test_importer.py .........                           [ 67%]
    unittests\test_librarysync.py ......                           [ 77%]
    unittests\test_providers.py ...                                [ 83%]
    unittests\test_setup.py ..........                             [100%]
    ================== 59 passed, 0 warnings in 10.68s ==================

In this example, 59 unit tests were run, across 5 source files. All of the tests passed, and completed running in 10 seconds.

### Interpreting pytest coverage results
After running pytest with code coverage enabled, you'll get something like the following:

    ---- coverage: platform win32, python 3.11.0-final-0 ----------
    Name                                        Stmts   Miss  Cover
    ---------------------------------------------------------------
    lazylibrarian\__init__.py                     275     68    75%
    lazylibrarian\api.py                         1537   1537     0%
    lazylibrarian\auth.py                          88     88     0%
    lazylibrarian\bookrename.py                   568    550     3%
    lazylibrarian\bookwork.py                    1075   1032     4%
    lazylibrarian\cache.py                        485    396    18%

This shows every source file that was included in the tests, how many statements were found and how many were missed. It's normally not economical to aim for 100% coverage, but something north of 70% is pretty good.

