wzdat_
======

**WzDat** is an acronym for `Webzen <http://www.webzen.com/main>`_ **Data Analysis Toolkit**, which started as an attempt to build `IPython <http://ipython.org>`_ & `Pandas <http://pandas.pydata.org>`_ based data analysis system. Currently WzDat consists of three applications. WzDat Python Module, Dashboard and `WzDat Forwarder for Windows <https://github.com/haje01/wdfwd>`_. and this repository is for WzDat Python Module & Dashboard.

To install, you need to clone from `Github repository`__

.. code-block:: console
    
    $ git clone git https://github.com/haje01/wzdat
    $ cd wzdat
    $ python setup.py install

__ https://github.com/haje01/wzdat

Since WzDat is a complex system where various software packages cooperate closely each other, putting all the pieces together is not a trivial work. So I decided to use `Docker <http://docker.com>`_ to solve the problem and, for sanitary reason, made another repository `wzdat-sys <https://github.com/haje01/wzdat-sys>`_ for that.
