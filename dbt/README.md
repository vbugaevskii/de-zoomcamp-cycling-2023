## dbt

1. Create environment to run dbt:
```bash
conda create -n dbt-py38 python=3.8 -y
conda activate dbt-py38
pip install -r requirements.txt
```

2. Prepare dbt project:
```bash
dbt init      # init
dbt deps      # install dependencies
dbt debug     # check db connection
dbt compile   # compile models
```

3. Run manually:
```bash
# run stg_rides_info model
dbt run -m stg_rides_info
dbt run -m stg_rides_info --var 'is_test_run: false'

# run all models
dbt run --var 'is_test_run: false'
```