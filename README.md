# Airflow Docker Setup for Windows

## Struktur
- `docker-compose.yaml`: konfigurasi resmi Airflow 3.0.1 dengan Postgres, Redis, Scheduler, Worker, Webserver
- `.env`: environment variable dengan AIRFLOW_UID=50000 (default untuk Windows)
- folder `dags/`: tempat letak DAG kamu (ada contoh `example_dag.py`)
- folder `logs/` dan `plugins/`: kosong, tapi wajib ada

## Cara pakai

1. Pastikan Docker dan Docker Compose sudah terinstall
2. Buka terminal CMD/PowerShell di folder ini
3. Buat folder yang diperlukan (jika belum ada):
   ```
   mkdir dags
   mkdir logs
   mkdir plugins
   mkdir config
   ```
4. Jalankan init Airflow (buat db dll):
   ```
   docker compose up airflow-init
   ```
5. Jalankan Airflow:
   ```
   docker compose up
   ```
6. Buka browser di http://localhost:8080
7. Login dengan user `airflow` dan password `airflow`

## Contoh DAG

File contoh ada di `dags/example_dag.py`.

---