.. _limitations:

Known limitations
=================

TODO:

нельзя менять кол-во десятичных разрядов? или в агрегатора сделать что бы брал максимальную длину? 

При увеличении количества разделов, ранее сохраненные данные остаются храниться в соответствии с
прежними номерами разделов.

https://kafka.apache.org/documentation/#basic_ops_modify_topic

Be aware that one use case for partitions is to semantically partition data, and adding partitions
doesn't change the partitioning of existing data so this may disturb consumers if they rely on that
partition. That is if data is partitioned by hash(key) % number_of_partitions then this partitioning
will potentially be shuffled by adding partitions but Kafka will not attempt to automatically
redistribute data in any way.

Экспериментировать с количеством разделов после запуска в эксплуатацию опасно, так как даже
откатить не получится.

FDB

Keys cannot exceed 10,000 bytes in size. Values cannot exceed 100,000 bytes in size. Errors will be
raised by the client if these limits are exceeded.