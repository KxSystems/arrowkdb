# Arrowkdb null bitmap

## Problem

Previously arrowkdb ignored the null bitmap when reading or writing an arrow array. This was due to the following reasons: 

- Using the kdb null values will result in some strange corner cases.

- Mapping to kdb nulls would hurt the performance. 

 Users have requested that arrowkdb provides a null bitmap when reading an arrow array so that the user can use this array in their applications.

When reading an arrow array using arrowkdb the user can now choose to return the null bitmap as well as the data values. The shape of the null bitmap structure is exactly the same as the data structure.  It is then left to the user to interpret the two structures as appropriate for their application.

Note: there is currently no support for null bitmap with the writer functions. 


## Implementation

The null bitmap feature is supported when reading:

* Parquet files
* Arrow IPC files
* Arrow IPC streams

To demonstrate it we first use the null mapping support to create a Parquet file containing nulls (although you can read null bitmaps from files generated by other writers such as PyArrow):

```q
q)options:(``NULL_MAPPING)!(::;`bool`uint8`int8`uint16`int16`uint32`int32`uint64`int64`float16`float32`float64`date32`date64`month_interval`day_time_interval`timestamp`time32`time64`duration`utf8`large_utf8`binary`large_binary`fixed_size_binary!(0b;0x00;0x00;0Nh;0Nh;0Ni;0Ni;0N;0N;0Nh;0Ne;0n;0Nd;0Np;0Nm;0Nn;0Np;0Nt;0Nn;0Nn;"";"";`byte$"";`byte$"";`byte$""))
q)table:([]col1:0N 1 2; col2:1.1 0n 2.2; col3:("aa"; "bb"; ""))
q).arrowkdb.pq.writeParquetFromTable["file.parquet";table;options]
```


Each reader function in arrowkdb takes an options dictionary.  A new `WITH_NULL_BITMAP option has been added.  When this option is set the reader functions return a two item mixed list, rather than one (the data values and null bitmap):

```q
q)read_results:.arrowkdb.pq.readParquetToTable["file.parquet";(enlist `WITH_NULL_BITMAP)!enlist 1]
q)read_results
+`col1`col2`col3!(0 1 2;1.1 0 2.2;("aa";"bb";""))
+`col1`col2`col3!(100b;010b;001b)
```

The null bitmap is a separate structure to kdb:

```q
q)first read_results
col1 col2 col3
--------------
0    1.1  "aa"
1    0    "bb"
2    2.2  ""
q)last read_results
col1 col2 col3
--------------
1    0    0
0    1    0
0    0    1
```


## Limitations

- The use of a  null bitmap with the writer functions is not supported. 

- Since the null bitmap structure and data structure must have the same shape, arrow arrays which use nested datatypes (list, map, struct, union, dictionaries) where the parent array contains null values cannot be represented.  For example, an array with a struct datatype in arrow can have either null child field values or the parent struct value could be null.  The null bitmap structure will only reflect the null bitmap of the child field datatypes.
