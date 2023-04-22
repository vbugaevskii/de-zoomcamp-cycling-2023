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
- `S3Bucket` named `yandex-cloud-s3-bucket`.

4. Deploy commands:

```bash
prefect deployment build flows/etl_usagestats_to_s3.py:etl_usagestats_to_s3_multiple -n etl_usagestats_to_s3_multiple --cron '0 6 * * *' --param latest=1
prefect deployment apply etl_usagestats_to_s3_multiple-deployment.yaml

prefect deployment build flows/etl_bikepoints_to_s3.py:etl_bikepoints_to_s3  -n etl_bikepoints_to_s3 --cron '1 6 * * *'
prefect deployment apply etl_bikepoints_to_s3-deployment.yaml

prefect deployment build flows/etl_bikepoints_to_ch.py:etl_bikepoints_to_ch  -n etl_bikepoints_to_ch --cron '2 6 * * *'
prefect deployment apply etl_bikepoints_to_ch-deployment.yaml
```