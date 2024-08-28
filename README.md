# Assessment

## Task A todo
- We receive the rates as high frequency (assume updates for multiple
currency pairs every millisecond) structured data, similar to the data
in rates_sample.csv
  - [ ] Multiple currency pairs / ms
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

