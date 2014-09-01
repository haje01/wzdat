wzdat_
======


.. image:: https://coveralls.io/repos/haje01/wzdat/badge.png
  :target: https://coveralls.io/r/haje01/wzdat


**WzDat** is an acronym for `Webzen <http://www.webzen.com/main>`_ **Data Analysis Toolkit** (you may pronounce it like "What's that?"), which started as an attempt to build `IPython <http://ipython.org>`_ & `Pandas <http://pandas.pydata.org>`_ based data analysis system. Currently WzDat consists of three applications. WzDat Python Module, Dashboard and `WzDat Forwarder for Windows <https://github.com/haje01/wdfwd>`_. This repository is for WzDat Python Module & Dashboard.

To install, you need to clone from `GitHub repository`__

.. code-block:: console
    
    $ git clone https://github.com/haje01/wzdat
    $ cd wzdat
    $ python setup.py install

__ https://github.com/haje01/wzdat

Though you can install it,the package alone can't do the job. WzDat is a complex system where various softwares cooperate closely each other, putting all the pieces together is not a trivial work. So I decided to adopt `Docker <http://docker.com>`_ as a solution and, for sanitary reason, made another repository `wzdat-sys <https://github.com/haje01/wzdat-sys>`_ for that.

