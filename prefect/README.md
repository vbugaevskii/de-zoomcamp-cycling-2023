## Local

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

- `AWS Credentials` named `yandex-cloud-s3-credentials`;
- `S3Bucket` named `yandex-cloud-s3-bucket`;
- `SQLAlchemy Connector` named `yandex-cloud-clickhouse-connector`.

4. Deploy commands:

```bash
prefect deployment build flows/etl_usagestats_to_s3.py:etl_usagestats_to_s3_multiple -n etl_usagestats_to_s3_multiple --cron '0 6 * * *' --param latest=1
prefect deployment apply etl_usagestats_to_s3_multiple-deployment.yaml

prefect deployment build flows/etl_usagestats_to_ch.py:etl_usagestats_to_ch_multiple -n etl_usagestats_to_ch_multiple --cron '10 6 * * *' --param latest=1
prefect deployment apply etl_usagestats_to_ch_multiple-deployment.yaml

prefect deployment build flows/etl_bikepoints_to_s3.py:etl_bikepoints_to_s3  -n etl_bikepoints_to_s3 --cron '0 6 * * *'
prefect deployment apply etl_bikepoints_to_s3-deployment.yaml

prefect deployment build flows/etl_bikepoints_to_ch.py:etl_bikepoints_to_ch  -n etl_bikepoints_to_ch --cron '5 6 * * *'
prefect deployment apply etl_bikepoints_to_ch-deployment.yaml
```

5. You can forward port using:
```bash
ssh -i ~/.ssh/dezoomcamp -L 4200:localhost:4200 -Nf vbugaevskii@158.160.45.104
```