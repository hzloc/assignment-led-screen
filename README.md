# Assessment

## Task A todo
- We receive the rates as high frequency (assume updates for multiple
currency pairs every millisecond) structured data, similar to the data
in rates_sample.csv
  - [ ] Multiple currency pairs / ms
  - [x] Assumption: 20 pairs per ms => 20 * 1000 => 20k insert/s support
  - [x] **Write optimized** table, less indexes better write performance 
  - [x] Query cost: @110k Planning Time: **0.172 ms** | Execution Time: **162.916 ms**