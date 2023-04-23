terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 0.13"
}

# Ref: https://registry.terraform.io/providers/yandex-cloud/yandex/latest/docs
# Ref: https://cloud.yandex.ru/docs/iam/operations/authorized-key/create
provider "yandex" {
  service_account_key_file = "terraform-sa-key.json"
  cloud_id  = "b1gfog90cahpmkosk5cp"
  folder_id = "b1gjidbal573f10rma62"
  zone      = "ru-central1-a"
}

# Create bucket
# Ref: https://registry.terraform.io/providers/yandex-cloud/yandex/latest/docs/resources/storage_bucket
# Ref: https://cloud.yandex.ru/docs/iam/operations/sa/create-access-key
# Ref: https://cloud.yandex.ru/docs/storage/operations/buckets/create
# TODO: Think how credentials can be stored in a safe way
resource "yandex_storage_bucket" "bucket-dev" {
  access_key = "YCAJE9DB4v57TeIVr7vOUZX5s"
  secret_key = "YCMOAAKp7LYPNbO_dw9PaVsftyvl-sETFCt_RFrP"
  bucket     = "de-zoomcamp-cycling-dev"
}

# Create Clickhouse instead of BigQuery. BigQuery is only available at GCP
# Ref: https://registry.terraform.io/providers/yandex-cloud/yandex/latest/docs/resources/mdb_clickhouse_cluster
resource "yandex_mdb_clickhouse_cluster" "ch_database" {
  name        = "clickhouse-cycling-dev"
  environment = "PRESTABLE"
  network_id  = "enpaigatk40k4je5bknq"

  clickhouse {
    resources {
      resource_preset_id = "b1.micro"
      disk_type_id       = "network-ssd"
      disk_size          = 10
    }
  }

  database {
    name = "sandbox"
  }

  database {
    name = "datamart"
  }

  # TODO: Think how credentials can be stored in a safe way
  user {
    name     = "admin"
    password = "ch_password"
    permission {
      database_name = "sandbox"
    }
    permission {
      database_name = "datamart"
    }
  }

  host {
    type = "CLICKHOUSE"
    zone = "ru-central1-a"
    subnet_id = "e9b1ado4iq8ld0eohi8f"
  }

  cloud_storage {
    enabled = false
  }

  maintenance_window {
    type = "ANYTIME"
  }

  access {
    web_sql = true
    data_lens = true
    metrika = true
    serverless = true
    yandex_query = true
    data_transfer = true
  }
}
