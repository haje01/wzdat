Installation
============

Tryout
------

If you're to meddle with WzDat's python module, just clone GitHub repository:

.. sourcecode:: console

   $ git clone https://github.com/haje01/wzdat
   $ cd wzdat
   $ python setup.py install

But the package alone can't do the real job. WzDat is a complex system where various softwares cooperate closely each other. Since setting all the pieces together is not a trivial work, I decided to adopt `Docker <http://docker.com>`_ as a solution and, for sanitary reason, made another repository `wzdat-sys <https://github.com/haje01/wzdat-sys>`_ for that.

Docker Way
----------

After cloning ``wzdat-sys`` 

.. sourcecode:: console
   $ git clone https://github.com/haje01/wzdat-sys
   $ cd wzdat-sys
   $ sys/build.sh
   $ WZDAT_HOST=(server-host-name)\
   $ ZDAT_DATA_DIR=(data-folder)\
   $ WZDAT_SOL_DIR=(solution-folder)\
   $ WZDAT_SOL_PKG=(solution-package-name)\
   $ WZDAT_PRJ=(project-id)\
   $ WZDAT_IPYTHON_PORT=(ipython-port)\
   $ WZDAT_DASHBOARD_PORT=(dashboard-port)\
   $ sys/run.sh
