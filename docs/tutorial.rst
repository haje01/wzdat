.. _tutorial

Tutorial
========

Let's have a quick glimpse of WzDat. In this tutorial, we're going to build a simple solution & project to analyze Linux Syslog.


Build Docker Container
----------------------

Make Solution & Project
-----------------------


Data Directory
--------------


Deploy By Docker
----------------

After you are done with the solution & project, clone ``wzdat-sys`` and build local docker image.

.. sourcecode:: console

   $ git clone https://github.com/haje01/wzdat-sys
   $ cd wzdat-sys
   $ sys/build.sh
   
Replace (..) variables with your own, then run script.

.. sourcecode:: console

   $ WZDAT_HOST=my.host.com\
   $ WZDAT_DATA_DIR=/home/myhome/data\
   $ WZDAT_SOL_DIR=/home/myhome/ws_mysol\
   $ WZDAT_SOL_PKG=ws_mysol\
   $ WZDAT_PRJ=myprj\
   $ WZDAT_IPYTHON_PORT=8085\
   $ WZDAT_DASHBOARD_PORT=8080\
   $ sys/run.sh
   

Select, Find And Analyze
------------------------

Expose Result To Dashboard
--------------------------
