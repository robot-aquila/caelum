.. _basics:

Basic concepts
==============

These are just few terms you have to know to understand how it works:

.. contents::
    :local:
    :depth: 2

****

Symbol
------

A *Symbol* is a string-type natural key representing a single event type. For example it can be a stock-exchange
security ticker or sensor ID or any other string to make possible distinguish one data source (or data meaning) from
other. *Symbol* is used to logically group the data of same event type together. To access event data in any kind of its
representation via **Caelum** a *symbol* is mandatory parameter. **Caelum** automatically tracks incoming *symbols* and
stores it in special directory. Also *symbols* can be stored to the directory explicitly by calling API method. Looking
ahead, you cannot obtain the list of *symbols* without specifying a *category*.

Category
--------

Because we said there thousands of event types are possible that mean we will face with lot of *symbols*. To make
navigation easier there are *categories*. *Category* is the string-type identifier that allow to group *symbols*
together. *Category* cannot be assigned manually but you can assign *Category Extractor* that will responsible to assign
*category* (or *categories*) for incoming events. By default **Caelum** consider that each *symbol* contains
information about *category*: the part of *symbol* from start up to *@* character considering as *category*. If *@*
character is not present then empty *category* will be assigned. With minor development efforts you can assign your own
*category extractor* with specific logic. For example with default *category extractor* there will be new *category*
in the system if at least one event of symbol *NYSE@AAPL* came. The *category* is *NYSE*. You can get the list of all
available *categories* and *symbols* belong to concrete *category* via API call. 

Item
----

An *item* is main food of the system. You should feed the system with *items* to get it work. An *item* represents of a
single event that comes in and should be stored and processed inside the system. *Item* is just a tuple of four
components: *symbol*, time, value and volume. *Symbol* is described above. Time of *item* is the time when event is
happened. Value is something valuable (like price or sensor reading). And volume is optional quantitative measurement
(like quantity of purchase). You should provide such data to make it stored and get aggregates as output. Depends on
configuration you can get *items* stored forever or just for particular period. Available *items* can be requested via
special API call.

Tuple
-----

A *Tuple* is a result of aggregation *items* for particular period of time. *Tuple* calculation based on *items*
belonging to the same period of time that *tuple* is. These are four elements based on *items* value movement: opening,
high, low and closing values; total sum of volume of all *items* included to the period, *symbol* (as described above)
and time of beginning particular time period. Depends on configuration *tuples* can be produced and stored by **Caelum**
automatically. If at least one aggregator is started **Caelum** could produce *tuples* of bigger time frames on-the-fly
(definitely it will cost you performance). So you can configure to run aggregators for particular periods for example
1 minute, 5 minutes, 6 hours or whatever you want depending on case and **Caelum** will produce
`OHLC+Volume <https://en.wikipedia.org/wiki/Open-high-low-close_chart>`__ *tuples* for you. In case if there is
real-time consumers of *OHLC* data the output stream can be forwarded to another software or retranslated via
thin software layer especially developed for that purposes. *Tuples* of particular *symbol* for particular time frame
and period of time can be obtained via API call.

Symbol update
-------------

*Items* and *tuples* are for aggregation and time series. But what if the data cannot be aggregated? What is there are
some attribute of *symbol* that can be changed with time but it is string or boolean value? **Caelum** offers solution
for such cases - the sequence of updates of symbol. Consider *symbol updates* as sequence of changes of secondary
attributes of data source where each update provides information of when, what attributes has been changed and which
new values they are got. Each *symbol update* contains *symbol*, time of update happened and map of key->values
representing the changes. For example if you operate stock-exchange market data and there is an initial margin attribute
of symbol which changes from time to time it can be stored as *symbol update*. Tracking all updates from beginning
will give the actual state of *symbol* attributes at the end of time. Tracking updates from beginning up to particular
time will give a snapshot of attributes at specific time. *Symbol updates* can be stored and obtained via API calls.

Feeder
-------

TODO:

Backnode
---------

TODO:

