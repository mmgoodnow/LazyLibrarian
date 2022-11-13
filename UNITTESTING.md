#
# November 2022
#
#Starting to introduce some unit testing using the simple unittest tools

#
#Requires
#
unittest for python

 sudo apt-get install python-setuptools


#
#Execution
#- run from the Lazylibrarian root directory (directory where LazyLibrarian.py exists)
python -m unittest discover unittests/

#From VSCode
Configure unittests as the directory to use
Run from within VSCode

expected output looks as follows (the two periods indicate 2 tests run)
..
----------------------------------------------------------------------
Ran 2 tests in 0.204s

OK
