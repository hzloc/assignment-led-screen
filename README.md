# Assessment

## Task B todo
- We receive the rates as high frequency (assume updates for multiple
currency pairs every millisecond) structured data, similar to the data
in rates_sample.csv
  - [x] 1 min updates
  - [x] Active rate
  - [x] Support 300 currency at same time (not tested but theoretically has to work)
  - [x] Display led with rate
  - [ ] Display led with change 


## to run

To run on docker:
```shell
build.sh
```
After done to destroy:
```shell
teardown.sh
```