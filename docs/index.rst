.. Rubix documentation master file, created by
   sphinx-quickstart on Fri Sep  1 12:04:37 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to RubiX's documentation!
=================================

RubiX is a light-weight data caching framework that can be used by Big-Data engines.

RubiX uses local disks to provide the best I/O bandwidth to the Big Data Engines. RubiX is useful in shared storage architectures where the data
execution engine is separate from storage. For example, on public clouds like AWS or Microsoft Azure, data is stored
in cloud store and the engine accesses the data over a network. Similarly in data centers `Presto <https://prestosql.io>`_
runs on a separate cluster from HDFS and accesses data over the network.

RubiX can be extended to support any engine that accesses data in cloud stores using Hadoop FileSystem interface via plugins.
There are plugins to access data on AWS S3, Microsoft Azure Blob Store, Google Cloud Storage and HDFS. RubiX can be extended to be
used with any other storage systems including other cloud stores.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   intro.md
   install/index.rst
   configuration.rst
   metrics.rst
   contrib/index.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
