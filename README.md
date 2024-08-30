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

