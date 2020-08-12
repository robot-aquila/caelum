.. _index:

Caelum Overview
===============

**Caelum** is a scalable, low-latency, high-throughput data aggregator that intended to collect,
process and store huge amount of events like stock-exchange market data, monitoring data, sensors data or telemetry
to track state changes with time and aggregate data into
`OHLCV-tuples <https://en.wikipedia.org/wiki/Open-high-low-close_chart>`__
for further analysis. Built on top of `Apache Kafka <https://kafka.apache.org/>`__ and
`Apple FoundationDB <https://www.foundationdb.org/>`__ it is flexible, durable and reliable.
**Caelum** has `microservice architecture <https://en.wikipedia.org/wiki/Microservices>`__ to make integration with
other software fast and easy. It can work on single host as well as in cluster depends on your needs and resource
availability.

**Caelum** designed as solution in case of:

- Thousands of data sources and consumers
- Dozens of thousands of event types
- Billions incoming events per day
- Time-critical data processing with millisecond precision
- Processing of critical data without any losses
- Terabytes of data that should be stored for years

**Caelum** can be used in trading software, stock-exchange and cryptocurrency exchanges, IoT software development,
DevOps, charting software and so on. 

Start from :ref:`Basic concepts <basics>` if you want to get deeper into the idea.
To get information about installation and setup read :ref:`Getting started <getstart>`.
Complete overview of the documentation is :ref:`here <contents>`.
