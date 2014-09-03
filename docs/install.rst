Installation
============

Tryout
------

If you're up to meddle with WzDat's python module, just clone GitHub repository:

.. sourcecode:: console

   $ git clone https://github.com/haje01/wzdat
   $ cd wzdat
   $ python setup.py install

But the package alone can't do the real job. WzDat is a complex system where various softwares cooperate closely each other. Since putting all the pieces together is not a trivial work, I decided to adopt `Docker <http://docker.com>`_ as a solution and, for sanitary reason, made another repository `wzdat-sys <https://github.com/haje01/wzdat-sys>`_ for that.

By using ``wzdat-sys``, setting a new WzDat server is in a twinkle. However, there are things to be prepared beforehand.


Regular
-------
WzDat requires compliant solution & project to use it as data analysis system. If this is your first time to setup WzDat, visit :ref:`tutorial` page to get aquaint with solution & project.

After you are done with making solution & project, clone ``wzdat-sys`` and build local docker image.

.. sourcecode:: console

   $ git clone https://github.com/haje01/wzdat-sys
   $ cd wzdat-sys
   $ sys/build.sh
   
Replace (..) variables with your own, then run script.

.. sourcecode:: console

   $ WZDAT_HOST=(server-host-name)\
   > WZDAT_DATA_DIR=(data-folder)\
   > WZDAT_SOL_DIR=(solution-folder)\
   > WZDAT_SOL_PKG=(solution-package-name)\
   > WZDAT_PRJ=(project-id)\
   > WZDAT_IPYTHON_PORT=(ipython-port)\
   > WZDAT_DASHBOARD_PORT=(dashboard-port)\
   > sys/run.sh
