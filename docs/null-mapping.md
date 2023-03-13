# Arrowkdb null mapping

## Problem 

Previously arrowkdb ignored the null bitmap when reading or writing an arrow array. Users have requested that arrowkdb maps arrow nulls into kdb.  

Unlike arrow, not all kdb types have a null value and those that do overload one value in the range (typically INT_MIN or FLOAT_MIN). 

For example:

- Each item in an arrow boolean array can be 0b, 1b or NULL.  kdb has no boolean null.  

- kdb doesn't have a byte null. 

- Unlike arrow, kdb can't distinguish between:
  - a null string and empty string.  
  - the " " character and null.


## Implementation

When reading and writing an arrow array using arrowkdb the user can now choose whether to map arrow nulls. Each reader and writer function in arrowkdb takes an options dictionary.  A new `NULL_MAPPING option has been added which allows the user to specify whether an arrow datatype should be null mapped and what value to use for null in kdb.  

For example:

```q
q)options[`NULL_MAPPING]
int16            | 0Nh 
int32            | 0Ni 
int64            | 0N 
float32          | 0Ne 
float64          | 0n 
date32           | 0Nd 
date64           | 0Np 
month_interval   | 0Nm 
day_time_interval| 0Nn 
timestamp        | 0Np 
time32           | 0Nt 
time64           | 0Nn 
duration         | 0Nn 
utf8             | "" 
binary           | `byte$() 
```

The type of each value in this dictionary must be the atomic type of the corresponding list representation for that datatype.  Where a datatype isn't present in this dictionary, arrowkdb will ignore the null bitmap (as per the previous behaviour).


## Limitations

- There is no null mapping for arrow arrays which use nested datatypes (list, map, struct, union, dictionaries) where the parent array contains null values.  For example, an array with a struct datatype in arrow can have either null child field values or the parent struct value could be null.  Arrowkdb will only map nulls for the child fields using the above mapping.

-  There is a loss of performance when choosing to map nulls, but this should not be significant. 
