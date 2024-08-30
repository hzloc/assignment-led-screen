# Assessment
## **Each task has its own branch**


## Task A todo
- We receive the rates as high frequency (assume updates for multiple
currency pairs every millisecond) structured data, similar to the data
in rates_sample.csv
  - [x] Multiple currency pairs / ms
  - [x] Assumption: 20 pairs per ms => 20 * 1000 => 20k insert/s support
  - [x] **Write optimized** table, less indexes better write performance
  - [x] Expensive is refreshing the materialized views, acceptable since 1 time in hour
  - [x] Query cost: @110k Planning Time: **0.172 ms** | Execution Time: **~7 ms**

## to run
```shell
pip install -r requirements.txt
```
To run setup postgres database:
```shell
docker-compose up --build -d
```
The command below will start to populate database with random data for testing.
Please stop the 
```shell
python -m unittest discover -s tests
```
You can view the data using dbeaver, or any other tool that supports postgres.
The output is saved in **conversion_rates_led_table_mv**, it's a materialized view.


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

To setup kafka:
```shell
docker-compose up --build
```
To download requirements:
```shell
pip install -r requirements.txt
```
To populate the exchange rates please run and run analysis
```shell
 python -m src.producer
```
To see the Led table please run:
```shell
python -m src.consumer
```

To clean 
```shell
teardown.sh
```

 