CSVMod
=========

Lets you programmatical modify csv files.

Usage:
`python csvmod.py controller.DemoController input.csv output.csv`

Where `controller.DemoController` is a python class name, which will be automatically imported as required.
Only changed lines will be written to the output.

<img src="https://raw.githubusercontent.com/nnscr/csvmod/master/graph_en.png" alt="" />

The Controller
----------
The controller describes the work that will be done on the csv feed.
`demo.py` contains two examples.


Contributing
----------
This tool is licensed under the <a href="https://github.com/nnscr/csvmod/blob/master/LICENSE">MIT license</a>. If you want to contribute,
commit your pull request or open an issue on GitHub.


Unit Tests
----------
You can run the unit test suite by using:
`python test.py`
