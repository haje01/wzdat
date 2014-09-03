.. _solandprj:

Solution And Projects
=====================

Your WzDat solution is a place where your projects exist. Your WzDat projects are place for your domain specific **file adapters** and utilities reside.

.. note::

   There are many log/data formats. They have different file naming rule, date time and log level format. To mitigate these differences, **file adapter** plays role. Refer to the following section for further details.
   
Let's have a look at example::

   ws_mysol/
      ws_mysol/
         __init__.py
         myprj/
            __init__.py

In this case, your solution name is ``mysol`` (Preferably, solution nams is from your personal name, or company name), then make topmost ``ws_mysol`` as solution folder, and inner ``ws_mysol`` as **solution python package**. 

.. tip::

   I advise you to start your solution with ``ws_`` prefix(``ws`` stands for **WzDat Solution**).

In the solution package, there is a WzDat **project python package** ``myprj``.
