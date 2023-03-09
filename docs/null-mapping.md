# Arrowkdb null mapping

## Background

The basic unit for storing data in arrow is an array.  Each array contains: 

- Datatype identifier 

- Length 

- Block of data (length as above) and accessors 

- A null bitmap (length as above)

Arrowkdb converts an arrow array to a kdb list and vice versa with type mapping as required: 

- Simple datatypes (ints, floats) are memcpy-ed 

- Temporal datatypes are copied one item at a time with the appropriate epoch offsetting and scaling 

- String and binary datatypes are copied into a mixed list of char or byte lists 

- Nested datatypes (list, map, struct, union, dictionaries) are represented by a mixed list of sublists, depending on the child datatypes (using recursion to populate the child lists)

Full details are provided here https://code.kx.com/q/interfaces/arrow/arrow-types/

# Mapping arrow nulls to kdb nulls

## Approach

Currently the simple datatype arrays are memcpy-ed as follows:

```cpp
  case arrow::Type::UINT16:
  {
    auto uint16_array = std::static_pointer_cast<arrow::UInt16Array>(array_data);
    memcpy(kH(k_array), uint16_array->raw_values(), uint16_array->length() * sizeof(arrow::UInt16Array::value_type));
    break;
  }
```

A simple approach to map arrow nulls to kdb nulls is to change this to:

```cpp
  case arrow::Type::INT16: 
  {
    auto int16_array = std::static_pointer_cast<arrow::Int16Array>(array_data);

    for (auto i = 0; i < int16_array->length(); ++i)
      if (int16_array->IsNull(i))
        kH(k_array)[i] = INT16_MIN;
      else
        kH(k_array)[i] = int16_array->Value(i);
    break;
  }
```

The issue with this is that it would result in a significant drop in performance due to inevitable failures in branch prediction.  However, with some arithmetic trickery the same functionality can be modelled without a branch:

```cpp
  case arrow::Type::INT16:
  {
    auto int16_array = std::static_pointer_cast<arrow::Int16Array>(array_data);
    for (auto i = 0; i < int16_array->length(); ++i)
      kH(k_array)[i] = (int16_array->IsNull(i) * INT16_MIN) + (!int16_array->IsNull(i) * int16_array->Value(i));
    break;
  }
```

Although there would still be a loss of performance (memcpy copies 64 bits at a time so doing an item by item copy where the datatype < 64 bits will be slower, plus the overhead of indexing into the null bitmap), that loss should not be significant. 

Note: the examples above refer to the reader functions but similar functionality would be provided for the writer functions which would perform the reverse operation (setting the arrow null bitmap if the value is a kdb null).

## Considerations

The bigger problem is that unlike arrow, not all kdb types have a null value.  Also, those that do just overload one value in the range (typically INT_MIN or FLOAT_MIN). 

For example:

- Each item in an arrow boolean array can be 0b, 1b or NULL.  kdb has no boolean null.  

- kdb doesn't have a byte null. 

- Unlike arrow, kdb can't distinguish between a null string and empty string.  Similarly it can't distinguish between the " " character and null.

Therefore mapping arrow nulls to kdb nulls is going to result in corner cases which can't be represented accurately.  However, the type mapping elsewhere in arrowkdb already has corner cases:

- db has no unsigned integer types.  Arrow unsigned ints are represented in kdb as signed ints.  Therefore if the top bit of an unsigned is set in arrow, it will display as a negative number in kdb. 

- Converting from kdb temporals to arrow temporals can result in a loss of precision.

A compromise would be to allow the user to specify how arrowkdb should map nulls.  Each reader and writer function in arrowkdb takes an options dictionary.  A new `NULL_MAPPING` option would be added which allows the user to specify whether an arrow datatype should be null mapped and what value to use for null.  

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

The type of each value in this dictionary must be the atomic type of the corresponding list representation for that datatype.  Where a datatype isn't present in this dictionary, arrowkdb would ignore the null bitmap (as per the existing behaviour).

Note: There is no null mapping for arrow arrays which use nested datatypes (list, map, struct, union, dictionaries) where the parent array contains null values.  For example, an array with a struct datatype in arrow can have either null child field values or the parent struct value could be null.  Arrowkdb will only map nulls for the child fields using the above mapping.

## Conclusions

Mapping arrow nulls to kdb nulls is considerably easier to implement and more intuitive for a kdb user.

Therefore, subject to review of this document by users, the better choice is null mapping.
