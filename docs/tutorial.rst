Tutorial
========

Let's have a quick glimpse of WzDat. In this tutorial, we're going to build a simple solution & project to analyze Linux Syslog.


Requirement
-----------

WzDat has been developed and tested on dockerized Linux(Ubuntu). You can deploy WzDat server under OSX or Windows host by using tools like Boot2Docker, but unknown defects could arise.

.. warning:: 

   Mounting directory from OSX or Windows to docker container would not work.


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
