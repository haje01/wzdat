Foreword
========

If you are a Python enthusiast or data scientist, you might have been heard about the `IPython <http://ipython.org>`_ and `Pandas <http://pandas.pydata.org>`_. Mixing them together forms one of the most beloved tools in this data centric era. IPython gives us nice interactive Python programming environment, and Pandas provides easy-to-use professional data tools. They are convinient and powerful tools not only for casual data work, but for big companies and laboratories. WzDat was started to empower this greate tool even more.

Profits
-------

Let's say you have thousands of log files, want to find some of them by their traits and specific words it contains, then feed the result to Pandas for more delicate analysis work. How would you do that? Some scripts or shell commands might be helpful, but the process is(in general) cumbersome and error prone to repeat everytime you get new files.

That's where WzDat comes in handy. Once WzDat Python module & its compliant project are imported under the IPython notebook environment, It will pick out your target files with impressive speed even from hundreds of thounds of files.

When you finished your analysis work in the IPython notebook, you can forwarding your outcome to WzDat Dashboard, from where your collegues share your work instantly with concise web interface.

Docker Depedent
---------------

`Docker <http://docker.com>`_ is a great platform which solves complex deploy problems. Since WzDat is one of the building blocks to construct an IPython & Pandas based data analysis system, there are much more softwares, configs, admin works to build one. To make deployment simpler, I choose Docker as default platform. Though it may sound strange, WzDat is premised on Docker, and its code presumes certain environments(folders, files, packages) provided by its Docker container.
