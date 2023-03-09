# Arrowkdb null bitmap

## Problem

Arrowkdb ignores the null bitmap when reading or writing an arrow array.  Even if the null bitmap is set for an array item, arrowkdb still reads its value from the array data.  This was done for a couple of reasons: 

- Kdb doesn't have proper distinct null values for its types.  Using the kdb null values will result in some strange corner cases.  The only way to do this properly would be to expose the array data separately to the null bitmap (in line with how arrow represents nulls).  But this would make the API more complex. 

- Mapping to kdb nulls would hurt the performance.  For simple types (ints, floats, etc.) arrowkdb bulk copies the entire arrow array into a kdb list using memcpy.  Having to check every array item for null then either use its value or the closest kdb null would require processing one item at a time. 

However, null support in arrowkdb has been requested by users so potential implementations are to be considered.

# Exposing the null bitmap to the kdb user

## Approach

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

In order to avoid the limitations described above with kdb nulls, an alternative approach is to expose the null bitmap as a separate structure to kdb (more in line with how arrow represents nulls):

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

The shape of the null bitmap structure would be exactly the same as the data structure.  It is then left to the user to interpret the two structures as appropriate for their application.

Each reader function in arrowkdb takes an options dictionary.  A new `WITH_NULL_BITMAP option would be added.  When this option is set the reader functions then return a two item mixed list (the data values and null bitmap):

```q
q)read_data_structures:.arrowkdb.pq.readParquetToTable["file.parquet";(``WITH_NULL_BITMAP)!((::);1)] 
q)read_data_structures 
+`col1`col2`col3!(-239800692 -930424766 1760748068i;-1.16675e+18 4.413091e+18.. 
+`col1`col2`col3!(011b;110b;010b)
```

Note: it would not be possible to support the null bitmap with the writer functions without a significant rework of arrowkdb.  This is because arrow arrays are built append only, it would not be possible to populate the values with a first pass (as done currently) then populate the null bitmap with a second pass.  Rather it would be necessary to populate the data values and null bitmap in a single pass which is not possible with the current design.

## Considerations

This approach results in an overly complicated API and would be unintuitive for kdb users (who are more familiar with and expect kdb nulls). 

Furthermore, how would the null bitmap be used in a kdb application?  If its only purpose is to populate the data structure with kdb nulls then it will suffer the same limitations as having arrowkdb do this mapping, while introducing unnecessary complexity. 

Note: Since the null bitmap structure and data structure must have the same shape, arrow arrays which use nested datatypes (list, map, struct, union, dictionaries) where the parent array contains null values cannot be represented.  For example, an array with a struct datatype in arrow can have either null child field values or the parent struct value could be null.  The null bitmap structure will only reflect the null bitmap of the child field datatypes.

## Conclusions

Exposing the null bitmap to the kdb user, while closer to how arrow represents null, makes the API overly complex and it isn't clear whether there is a clear use case where the null bitmap could be well utilised in kdb.  Also, given the additional complexity of exposing the null bitmap, there may be other issues or corner cases which only become evident during development.
