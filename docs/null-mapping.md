# Arrowkdb null mapping

## Problem 

Previously arrowkdb ignored the null bitmap when reading or writing an arrow array. Users have requested that arrowkdb maps arrow nulls into kdb.  

Unlike arrow, not all kdb types have a null value and those that do overload one value in the range (the 0N* values typically map to INT_MIN or FLOAT_MIN). 

For example:

- Each item in an arrow boolean array can be 0b, 1b or NULL.  kdb has no boolean null.  

- kdb doesn't have a byte null. 

- Unlike arrow, kdb can't distinguish between:
  - a null string and empty string.  
  - the " " character and null.


## Implementation

When reading and writing an arrow array using arrowkdb the user can now choose whether to map arrow nulls. Each reader and writer function in arrowkdb takes an options dictionary.  A new `NULL_MAPPING option containing a dictionary of datatypes > null values has been added which allows the user to specify whether an arrow datatype should be null mapped and what value to use for null in kdb.

> :warning: **An identify function (::) may be required in the options dictionary values**
>
> The options dictionary values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.  Therefore if the only set option is NULL_MAPPING, an additional empty key and corresponding value identity function (::) must be included in the options to make the values a mixed list.

The following Arrow datatype are supported, along with possible null mapping values:

```q
q)options:(``NULL_MAPPING)!(::;`bool`uint8`int8`uint16`int16`uint32`int32`uint64`int64`float16`float32`float64`date32`date64`month_interval`day_time_interval`timestamp`time32`time64`duration`utf8`large_utf8`binary`large_binary`fixed_size_binary!(0b;0x00;0x00;0Nh;0Nh;0Ni;0Ni;0N;0N;0Nh;0Ne;0Nf;0Nd;0Np;0Nm;0Nn;0Np;0Nt;0Nn;0Nn;"";"";`byte$"";`byte$"";`byte$""))
q)options`NULL_MAPPING
bool             | 0b
uint8            | 0x00
int8             | 0x00
uint16           | 0Nh
int16            | 0Nh
uint32           | 0Ni
int32            | 0Ni
uint64           | 0N
int64            | 0N
float16          | 0Nh
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
large_utf8       | ""
binary           | `byte$()
large_binary     | `byte$()
fixed_size_binary| `byte$()
```

The type of each value in this dictionary must be the atomic type of the corresponding list representation for that datatype.  Where a datatype isn't present in this dictionary, arrowkdb will ignore the null bitmap (as per the previous behaviour).

## Example

Using these null mapping we can pretty print an arrow arrow where the kdb nulls have been mapped to arrow nulls:

```q
q)options:(``NULL_MAPPING)!(::;`bool`uint8`int8`uint16`int16`uint32`int32`uint64`int64`float16`float32`float64`date32`date64`month_interval`day_time_interval`timestamp`time32`time64`duration`utf8`large_utf8`binary`large_binary`fixed_size_binary!(0b;0x00;0x00;0Nh;0Nh;0Ni;0Ni;0N;0N;0Nh;0Ne;0n;0Nd;0Np;0Nm;0Nn;0Np;0Nt;0Nn;0Nn;"";"";`byte$"";`byte$"";`byte$""))
q)table:([]col1:0N 1 2; col2:1.1 0n 2.2; col3:("aa"; "bb"; ""))
q).arrowkdb.tb.prettyPrintTableFromTable[table;options]
col1: int64
col2: double
col3: string
----
col1:
  [
    [
      null,
      1,
      2
    ]
  ]
col2:
  [
    [
      1.1,
      null,
      2.2
    ]
  ]
col3:
  [
    [
      "aa",
      "bb",
      null
    ]
  ]

q)
```




## Limitations

- There is no null mapping for arrow arrays which use nested datatypes (list, map, struct, union, dictionaries) where the parent array contains null values.  For example, an array with a struct datatype in arrow can have either null child field values or the parent struct value could be null.  Arrowkdb will only map nulls for the child fields using the above mapping.

-  There is a loss of performance when choosing to map nulls, but this should not be significant. 
