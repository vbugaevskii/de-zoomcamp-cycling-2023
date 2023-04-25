## Prefect

1. Create environment to run Prefect:

```bash
conda create -n prefect-py38 python=3.8 -y
conda activate prefect-py38
pip install -r requirements.txt
```

2. Run Prefect:

```bash
# start web viewer
prefect orion start

# start prefect agent for running deployments
prefect agent start --work-queue "default"
```

3. Create Prefect Blocks:

- `AWS Credentials` named `yandex-cloud-s3-credentials`:
  - Yandex.Cloud is compatible with AWS interface;
  - you will need [service account](https://cloud.yandex.ru/docs/iam/concepts/users/service-accounts) from previous homework and [access key](https://cloud.yandex.ru/docs/iam/concepts/authorization/access-key) for it;
  - use endpoint: https://storage.yandexcloud.net
- `S3Bucket` named `yandex-cloud-s3-bucket`:
  - use AWS credentials from previous step;
  - use endpoint: https://storage.yandexcloud.net
- `SQLAlchemy Connector` named `yandex-cloud-clickhouse-connector`:
  - use URI:
    ```
    clickhouse+http://admin:password@c-<clickhouse-cluster-id>.rw.mdb.yandexcloud.net:8443/default
    ```
    and additional connection arguments:
    ```json
    {
        "protocol": "https",
        "verify": "YandexCA.crt"
    }
    ```
- `Secret` named `ceda-archive-secret`:
  - retrieve `ceda.session.1` cookie from your browser.
- `dbt Core Operation` name `dbt-core`:
  - specify commands:
    ```json
    [
      "dbt debug",
      "dbt compile",
      "dbt run"
    ]
  ```

4. Deploy commands:

```bash
prefect deployment build flows/etl_usagestats_to_s3.py:etl_usagestats_to_s3_multiple -n etl_usagestats_to_s3_multiple --cron '0 6 * * *' --param latest=1
prefect deployment apply etl_usagestats_to_s3_multiple-deployment.yaml

prefect deployment build flows/etl_usagestats_to_ch.py:etl_usagestats_to_ch_multiple -n etl_usagestats_to_ch_multiple --cron '10 6 * * *' --param latest=1
prefect deployment apply etl_usagestats_to_ch_multiple-deployment.yaml

prefect deployment build flows/etl_bikepoints_to_s3.py:etl_bikepoints_to_s3 -n etl_bikepoints_to_s3 --cron '0 6 * * *'
prefect deployment apply etl_bikepoints_to_s3-deployment.yaml

prefect deployment build flows/etl_bikepoints_to_ch.py:etl_bikepoints_to_ch -n etl_bikepoints_to_ch --cron '5 6 * * *'
prefect deployment apply etl_bikepoints_to_ch-deployment.yaml

prefect deployment build flows/etl_weather_to_s3.py:etl_weather_to_s3_multiple -n etl_weather_to_s3_multiple --cron '5 6 * * *'
prefect deployment apply etl_weather_to_s3_multiple-deployment.yaml

prefect deployment build flows/etl_weather_to_ch.py:etl_weather_to_ch_multiple -n etl_weather_to_ch_multiple --cron '10 6 * * *' --param partions_num=[202112]
prefect deployment apply etl_weather_to_ch_multiple-deployment.yaml

prefect deployment build flows/trigger_dbt_flow.py:trigger_dbt_flow -n trigger_dbt_flow --cron '15 6 * * *'
prefect deployment apply trigger_dbt_flow-deployment.yaml
```

5. You should make the first run manually:

```bash
# process partitions for rides history
python flows/etl_usagestats_to_s3.py 
python flows/etl_usagestats_to_ch.py

# process bike points info
python flows/etl_bikepoints_to_s3.py 
python flows/etl_bikepoints_to_ch.py

# process partition for weather history
python flows/etl_weather_to_s3.py 
python flows/etl_weather_to_ch.py
```