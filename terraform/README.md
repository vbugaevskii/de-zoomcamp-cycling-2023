## Yandex.Cloud 

Unfortunatelly Google Cloud and AWS are not available in my country, so I will use Yandex.Cloud.

**Note:** all credentials below are not valid.

### Pre-Requisites

1. Terraform client installation: https://www.terraform.io/downloads
2. Yandex.Cloud CLI: https://cloud.yandex.ru/docs/cli/quickstart

### Preprare Cloud

1. Go to [Yandex.Cloud console](https://console.cloud.yandex.ru/) and create a new cloud for course, e.g. `data-engineering-zoomcamp`.

2. Create a service account, e.g. `storage-admin-sa` with:
- `storage.configurer` and `storage.admin` roles for buckets, see [here](https://cloud.yandex.ru/docs/iam/concepts/access-control/roles) and [here](https://cloud.yandex.ru/docs/storage/security/);
- `mdb.admin` role for ClickHouse (closest analogue to BigQuery), see [here](https://cloud.yandex.ru/docs/iam/concepts/access-control/roles) and [here](https://cloud.yandex.ru/docs/managed-clickhouse/security/).

3. Prepare credentials from service account for CLI: use [tutorial](https://cloud.yandex.ru/docs/tutorials/infrastructure-management/terraform-quickstart#get-credentials):
    
    **Note:** you can use the commands to view your resources:
    
    ```
    $ yc resource-manager cloud list
    +----------------------+---------------------------+----------------------+
    |          ID          |           NAME            |   ORGANIZATION ID    |
    +----------------------+---------------------------+----------------------+
    | b1gfog90cahpmkosk5cp | data-engineering-zoomcamp | bpfc7863kge62q9246j3 |
    +----------------------+---------------------------+----------------------+
    ```
    <br>
    
    ```
    $ yc resource-manager --cloud-id b1gfog90cahpmkosk5cp folder list
    +----------------------+---------+--------+--------+
    |          ID          |  NAME   | LABELS | STATUS |
    +----------------------+---------+--------+--------+
    | b1gjidbal573f10rma62 | default |        | ACTIVE |
    +----------------------+---------+--------+--------+
    ```
    <br>

    1. Create authorization key, see [the link](https://cloud.yandex.ru/docs/iam/concepts/authorization/key):
    <br></br>
    
    ```bash
    $ yc iam key create \
        --service-account-id aje12il832r8ngrvk34d \
        --folder-id b1gjidbal573f10rma62 \
        --output terraform-sa-key.json

    id: ajet6bikuoot3ag48gq4
    service_account_id: aje12il832r8ngrvk34d
    created_at: "2023-04-23T22:21:13.420126832Z"
    key_algorithm: RSA_2048
    ```
    <br>
    
    2. Create static access key, see [the link](https://cloud.yandex.ru/docs/iam/operations/sa/create-access-key):
    <br></br>
    
    ```bash
    $ yc iam access-key create \
        --service-account-id aje12il832r8ngrvk34d \
        --folder-id b1gjidbal573f10rma62
    
    access_key:
      id: ajepu9ce58uv0fmbvjdk
      service_account_id: aje12il832r8ngrvk34d
      created_at: "2023-04-23T22:22:00.551532938Z"
      key_id: <access_key>
    secret: <secret_key>
    ```
    
### Terraform commands

* `terraform init` – install provider plugins;
* `terraform plan` – preview execution pipeline;
* `terraform apply` – run execution pipeline;
* `terraform destroy` – destroy all objects created by pipeline.
