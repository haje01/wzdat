Requirements
============

Linux System
------------

WzDat has been developed and tested on dockerized Linux(Ubuntu). You can deploy WzDat server under OSX or Windows host by using tools like Boot2Docker, but unknown defects could arise.

.. warning:: 

   Mounting directory from OSX or Windows to docker container would not work.


WzDat Solution & Project
------------------------

Before using WzDat to analyze your data, you need your **solution** and **project**. Your WzDat solution is a place where your projects exist. Your WzDat projects are place for your domain specific **file adapters** and utilities reside.

Let's have a look at example::

   ws_mysol/
      ws_mysol/
         __init__.py
         myprj/
            __init__.py

In this case, your solution name is ``mysol`` (Preferably, solution nams is from your personal name, or company name), then make topmost ``ws_mysol`` as solution folder, and inner ``ws_mysol`` as solution python package. 

.. tip::

   I advise you to start your solution with ``ws_`` prefix(``ws`` stands for **WzDat Solution**).

In the solution package, there is a WzDat project python package ``myprj``.
