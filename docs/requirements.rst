Requirements
============

Linux System
------------

WzDat has been developed and tested on dockerized Linux(Ubuntu). You can deploy WzDat server under OSX or Windows host by using tools like Boot2Docker, but unknown defects could arise.

.. warning:: 

   Mounting directory from OSX or Windows to docker container would not work.


WzDat Solution & Project
------------------------

Before using WzDat to analyze your data, you need your **solution** and **project**. Your WzDat solution is a place where your projects exist. Your WzDat projects are place for your domain specific file adapters and utilities reside.

Let's have a look at sample solution & projects::

   ws_mysol/
      ws_mysol/
         __init__.py
         myprj/
            __init__.py

Let's say your solution name is ``mysol``, then make topmost ``ws_mysol`` as base folder, and inner ``ws_mysol`` as python package (folder has ``__init__.py`` file). 

.. tip::

   I advise you to start your solution with ``ws_`` prefix(``ws`` stands for **WzDat Solution**).
