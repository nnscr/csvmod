CSVMod
=========

Lets you programmatical modify csv files.

Usage:
`python csvmod.py controller.DemoController input.csv output.csv`

Where `controller.DemoController` is a python class name, which will be automatically imported as required.
Only changed lines will be written to the output.



The Controller
----------
The controller describes the work that will be done on the csv feed.
`controller.py` contains two examples.
