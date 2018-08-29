# Database Systems

## Implementation of mini MySQL Engine in Python.
**Prerequisite**
- Python 3.x
- metadata.txt file of the tables in the same folder as source
- .csv file of tables in the same folder as source

*Usage:*
```
python miniSQL.py [sql queries]
```

## Clause Implemented
- select
- from
- where
- aggregation functions {total aggregation(min, max, sum, avg, count) partial aggregation(distinct)}

## Query
**select** *[columns with aggregate functions]* **from** *[table/s with aliases]* **where** *[condition]*

## To-Do
- [x] select all columns from the given tables
- [x] project only selected columns from the given table
- [x] make a function that parses where
- [x] aggregate function
- [ ] make function that parses order by sorts according to given value
- [ ] optimize joining of two table queries
