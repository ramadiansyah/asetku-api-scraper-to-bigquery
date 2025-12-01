from google.cloud import bigquery
import json
import os

SERVICE_ACCOUNT_FILE = "/opt/airflow/config/gcs_keys.json"

# List nama dataset yang ingin dihapus (format: "project_id.dataset_id")
datasets_to_delete = [
    "purwadika.rr_nyc_default_rr_nyc_default",
    "purwadika.rr_nyc_default_rr_nyc_taxi_dw",
    "purwadika.rr_nyc_default_rr_nyc_taxi_mart",
    "purwadika.rr_nyc_default_rr_nyc_taxi_prep",
    "purwadika.rr_nyc_taxi_dw",
    "purwadika.rr_nyc_taxi_mart",
    "purwadika.rr_nyc_taxi_prep",

]

# Inisialisasi client BigQuery
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)

# Loop dan hapus setiap dataset
for dataset_full_id in datasets_to_delete:
    try:
        client.delete_dataset(
            dataset_full_id,
            delete_contents=True,  # Hapus semua tabel di dalamnya
            not_found_ok=True      # Lewati jika dataset tidak ditemukan
        )
        print(f"✅ Dataset '{dataset_full_id}' berhasil dihapus.")
    except Exception as e:
        print(f"❌ Gagal menghapus '{dataset_full_id}': {e}")
