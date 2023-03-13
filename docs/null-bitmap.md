# Arrowkdb null bitmap

## Problem

Previously arrowkdb ignored the null bitmap when reading or writing an arrow array. This was due to the following reasons: 

- Using the kdb null values will result in some strange corner cases.

- Mapping to kdb nulls would hurt the performance. 

 Users have requested that arrowkdb provides a null bitmap when reading an arrow array.so that the user can use this array in their applications.

When reading an arrow array using arrowkdb the user can now choose to return the null bitmap as well as the data values. The shape of the null bitmap structure is exactly the same as the data structure.  It is then left to the user to interpret the two structures as appropriate for their application.

Note: there is currently no support for null bitmap with the writer functions. 


## Implementation


Arrowkdb represents an arrow table (which is a set of arrow arrays) as a mixed list of lists.  This is then decorated with the field names from the schema to create a kdb table, similar to:

```q
q)field_names:`col1`col2`col3 
q)array_data:(3?0i;`float$3?0;3?0p) 
q)GetTable:{flip field_names!array_data} 
q)GetTable[] 
col1       col2         col3 
----------------------------------------------------- 
-239800692 -1.16675e+18 2003.05.24D03:45:53.202889856 
-930424766 4.413091e+18 2001.07.22D09:51:37.461634128 
1760748068 2.89119e+18  2001.07.26D01:03:47.039068936 
```


Each reader function in arrowkdb takes an options dictionary.  A new `WITH_NULL_BITMAP option has been added.  When this option is set the reader functions return a two item mixed list, rather than one (the data values and null bitmap):

```q
q)read_data_structures:.arrowkdb.pq.readParquetToTable["file.parquet";(``WITH_NULL_BITMAP)!((::);1)] 
q)read_data_structures 
+`col1`col2`col3!(-239800692 -930424766 1760748068i;-1.16675e+18 4.413091e+18.. 
+`col1`col2`col3!(011b;110b;010b)
```

The null bitmap is a separate structure to kdb:

```q
q)null_bitmaps:(3?0b;3?0b;3?0b) 
q)GetTableWithNulls:{((flip field_names!array_data);(flip field_names!null_bitmaps))} 
q)GetTableWithNulls[] 
+`col1`col2`col3!(-239800692 -930424766 1760748068i;-1.16675e+18 4.413091e+18.. 
+`col1`col2`col3!(011b;110b;010b) 
q)first GetTableWithNulls[] 
col1       col2         col3 
----------------------------------------------------- 
-239800692 -1.16675e+18 2003.05.24D03:45:53.202889856 
-930424766 4.413091e+18 2001.07.22D09:51:37.461634128 
1760748068 2.89119e+18  2001.07.26D01:03:47.039068936 
q)last GetTableWithNulls[] 
col1 col2 col3 
-------------- 
0    1    0 
1    1    1 
1    0    0 
```


## Limitations

- The use of a  null bitmap with the writer functions is not supported. 

- Since the null bitmap structure and data structure must have the same shape, arrow arrays which use nested datatypes (list, map, struct, union, dictionaries) where the parent array contains null values cannot be represented.  For example, an array with a struct datatype in arrow can have either null child field values or the parent struct value could be null.  The null bitmap structure will only reflect the null bitmap of the child field datatypes.
