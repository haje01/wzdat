.. _solandprj:

Solution And Projects
=====================

Your WzDat solution is a place where your projects exist. Your WzDat projects are place for your domain specific **file adapters** and utilities reside.

.. note::

   There are many log/data formats. They have different file naming rule, date time and log level format. To mitigate these differences, **file adapter** plays role. Refer to the following section for further details.
   
Let's start by a skeleton::

   ws_mysol/
      ws_mysol/
         __init__.py
         myprj/
            __init__.py


In this case, your solution name is ``mysol`` (Preferably, solution nams comes from your personal name, or company name), then make topmost ``ws_mysol`` as solution folder, and inner ``ws_mysol`` folder, and as **solution python package**. In the solution package, there is a WzDat **project python package** ``myprj``.

.. tip::

   I advise you to start your solution with ``ws_`` prefix(``ws`` stands for **WzDat Solution**).


File Adapter
------------

File adapter is a python module in which you implement functions to feed required information about the file format you are dealing. For example, if you want your WzDat project can handle two types of file( log, dblog ), your project might look like this::

   ws_mysol/
      ws_mysol/
         __init__.py
         myprj/
            __init__.py
            log.py     <--
            dbdump.py  <--


=========== ============== ==========
File Type   File Extension Adapter
=========== ============== ==========
Log         ``.log``       log.py
DB Dump     ``.csv``       dbdump.py
=========== ============== ==========

Each adapter moudule is asked to implement following functions:


Configs
-------

To feed setting information, config files are located at your solution and projects::

   ws_mysol/
      ws_mysol/
         __init__.py     <--
         config.yaml
         myprj/
            __init__.py
            config.yaml  <--
            log.py
            dbdump.py

If some settings are common among your projects, you can place them at solution config file. If some others are specific for a certain project, create project config file.


Notes Folder
------------
Finally, create ``__notes__`` folder to accomodate your IPython Notebooks. And create nested folder per project::

   ws_mysol/
      __notes__     <--
         myprj/     <--
      ws_mysol/
         __init__.py   
         config.yaml
         myprj/
            __init__.py
            config.yaml
            log.py
            dbdump.py

