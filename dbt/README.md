## dbt

1. Create environment to run dbt:
```bash
conda create -n dbt-py38 python=3.8 -y
conda activate dbt-py38
pip install -r requirements.txt
```

2. Don't forget to configure ClickHouse configuration in [`cycling/profiles.yml`](cycling/profiles.yml).

3. Prepare dbt project:
```bash
dbt init      # init
dbt deps      # install dependencies
dbt debug     # check db connection
dbt compile   # compile models
```

4. Run manually:
```bash
# run stg_rides_info model
dbt run -m stg_rides_info
dbt run -m stg_rides_info --var 'is_test_run: false'

# run all models
dbt run --var 'is_test_run: false'
```

5. Regular updates are provided by prefect, see deployment:
```bash
prefect deployment build flows/trigger_dbt_flow.py:trigger_dbt_flow -n trigger_dbt_flow --cron '15 6 * * *'
prefect deployment apply trigger_dbt_flow-deployment.yaml
```