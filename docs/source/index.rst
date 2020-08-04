.. _index:

Caelum Overview
===============

==================================== ==================================
:ref:`Table of Contents <contents>`  :ref:`Getting started <getstart>`
==================================== ==================================

**Caelum** is an open-source time-series database middleware that intended to collect, process and store huge amount
of events and their derivatives *Open-High-Low-Close-Volume* tuples for various intervals. Built on top of
`Apache Kafka <https://kafka.apache.org/>`__ and `Apple FoundationDB <https://www.foundationdb.org/>`__
**Caelum** is fast, scalable and reliable. It is flexible in setup and can work on single host as well as in cluster
depends on your needs. Initially **Caelum** is developed to store trade data from different stock-markets. But due
to well-design it can be used to store any events that can be represented by { symbol, time, value, volume } tuple
and should be aggregated as OHLCV tuples for future analysis.

There are several use cases by nature of data:

- Market data or any kind of auctions
- Sensor data and telemetry
- Monitoring data and DevOps
- Exchange rates

And several use cases by situation:

- Lot of data sources or/and consumers
- Huge amount of events or/and event types
- Time-critical data processing
- Operates critical data that must not be loss


