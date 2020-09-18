.. _binary_formats:

***********************
Двоичные форматы данных
***********************

Для ускорения обработки и сокращения объема используемых данных, некоторые данные сохраняются и передаются
в двоичном формате. В данном разделе описывается как данные представляются на низком уровне.


.. contents::
    :local:
    :depth: 2


Формат представления событий
============================

Двоичный формат представления событий используется при отправке и получении сообщений в топик событий кластера Kafka.
При этом, в качестве ключа используется символ, а в качестве времени сообщения - время слота событий.
Упаковываются только параметры событий.

По каждому событию формируется отдельная запись:

- Первый байт представляет собой заголовок пары.
    
    - бит 0 (*delete mark*) - если включен, указывает что это инструкция на удаление события. При этом, содержательная
      часть события (данные) не включается в запись. Если бит выключен, то это инструкция на добавление или обновление
      события. В этом случае запись содержит упакованные данные события.
    
    - биты 1-3 (*id bytes*) хранят длину идентификатора события в байтах минус один. То есть, 0 в этих трех битах
      указывает на длину в 1 байт. Когда все три бита включены (то есть значение 7), значит под идентификатор отводится
      8 байт.
	  
    - бит 4 (*reserved 1*) зарезервирован для будущего использования.
	
    - биты 5-7 (*data size bytes*) хранят количество байт длины содержательной части (длина строки данных) минус
      один. При этом, данные биты выключены, если *delete mark* включен.
	
- Следующие байты в количестве *id bytes* байт кодируют идентификатор события

Далее данные кодируются при условии, что *delete mark* выключен

- Следующие байты в количестве *data size bytes* кодируют длину содержательной части события. То есть, длину строки -
  *data size*.
- Следующие байты в количестве *data size* кодируют содержимое события

Записи всех событий слота объединяются в одну двоичную запись. Распаковка прекращается по достижения конца этой
комбинированной записи.


Basics
======

Symbol and time are keys and not considered as a part of packed binary record.
To avoid linking with external storages values are stored along with number of decimals.
To reach maximum compactness the records can be stored in different formats depends on actual data.
For example just 4 bytes may be enough to represent a lot of trades of typical stock-market security.
**Caelum** is optimized both to save the space and keep processing fast.


Item binary format
------------------

Item contains two components: *value* and *volume*.
Each record starts with header which encodes a record type.
First lower 2 bits (rightmost) are:

- 0x00 - Reserved
- 0x01 - LONG_COMPACT
- 0x02 - LONG_REGULAR
- 0x03 - Reserved

*LONG_COMPACT* is for items that repsesents *value* in range 0-65535 and *volume* in range 0-63 with number
of decimals (both components) in range 0-15. The rest 6 bits of the first byte is an item *volume* which
is in range 0-63 inclusive. The next byte encodes number of decimals: lower 4 bits of *value* and higher 4
bits of *volume*. The next two bytes is *value* 0-65535. The record size of this type is fixed: 4 bytes.
*LONG_COMPACT* is suitable for most typycal trades which is quoted in US dollars (or something like that)
and can save lot of space.

*LONG_REGULAR* is for items that represents *value* and *volume* in range from -2^63 to 2^63-1
with number of decimals in range 0-15. The rest 6 bits of the first byte are used to encode size of
*value* and *volume* in bytes: bits 2-4 and 5-7 respectively. The size of component (*value* or *volume*)
cannot be less than 1 byte. Maximum size of component is 8 bytes. To encode such range the size is reduced by
one. So the encoded size of 0 equals to size of 1 byte, 1 to size of 2 bytes and so on, 7 is size of 8
bytes. The next byte of *LONG_REGULAR* record encodes number of decimals: lower 4 bits of *value* and
higher 4 bits of *volume*. The next bytes are compacted representation of *value* (1-8 bytes). And the next
bytes are compacted *volume*. Minimal record size of this type is 4 bytes: 1 for header, 1 for number of
decimals, 1 for *value* and 1 for *volume*. The maximum record size is 18 bytes. 


Tuple binary format
-------------------

Tuple contains five components: *open*, *high*, *low* and *close* values (*OHLC*) and summary *volume* for
period of aggregation. Each record starts with header wich encodes a record type.
First 2 bits are:

- 0x00 - Reserved
- 0x01 - Reserved
- 0x02 - LONG_REGULAR or LONG_WIDEVOL
- 0x03 - Reserved

*LONG_REGULAR* is for tuples where components are in range from -2^63 to 2^63-1 and number of decimals
in range 0-15. *LONG_WIDEVOL* is mostly same except that *volume* can be greater size than 8 bytes.
For *LONG_REGULAR*  the next 3 bits (2-4) encodes the size of *volume* in bytes minus one. For *LONG_WIDEVOL*
bits 2-4 of the first byte are always on and size of *volume* determined by remaining length of the record.
If remaining length is greater than 8 then *BigInteger* type will be used to represent *volume*.
The last bits of the first byte are reserved (5-7). The next byte encodes number of decimals: lower 4 bits
of *OHLC* and higher 4 bits of *volume*.

The next byte encodes type and size of *open* and *high* components: 4 bits per component. The bit 0 is
reserved. Bits 1-3 encodes the size of *open* component in bytes minus 1. Bit 4 represents type of encoding
of *high* component: if bit is off then absolute value if bit is on then relative to *open* component.
Bits 5-7 are size of *high* in bytes minus 1. The next byte encodes type and size of *low* and *close*
components like as previous byte: 4 bits per component, the lower one bit is the type (0 - absolute, 1 -
relative to *open*), the rest 3 bits is the size in bytes minus 1. The next bytes are bytes of components
in order: *open*, *high*, *low*, *close*, *volume* respectively sizes from header.

The header total size is 4 bytes: record type and size of *volume*, number of decimals, *open* and *high*
params, *low* and *close* params. Minimal record size is 9 bytes. Maximum size is 44 if there is
*LONG_REGULAR* record type and unlimited if there is *LONG_WIDEVOL*. 

