# IDX Signals ELT Pipeline (Airflow + PostgreSQL)

## Deskripsi Proyek
Proyek ini merupakan pipeline ELT otomatis yang dirancang untuk mengambil data harga saham dari IDX (Bursa Efek Indonesia), melakukan transformasi data, dan menyimpannya ke dalam PostgreSQL menggunakan Apache Airflow.  
Tujuan utama proyek ini adalah membangun data pipeline yang dapat dijadwalkan dan diotomatisasi agar data selalu terbarui dan siap untuk analisis lebih lanjut.

---

## Arsitektur Proyek
Pipeline ini berjalan di dalam Docker environment menggunakan tiga komponen utama:
1. **PostgreSQL** sebagai penyimpanan data mentah, staging, dan hasil transformasi.
2. **Apache Airflow** sebagai pengatur alur ELT (Extract, Transform, Load) dan penjadwalan otomatis.
3. **Python Scripts (ELT)** sebagai proses pengambilan dan pembersihan data.

**Struktur folder:**
IDX-SIGNALS/
│
├── airflow/
│ ├── dags/
│ │ ├── idx_prices_hourly.py # DAG untuk proses extract data harga saham per jam
│ │ └── idx_features_hourly.py # DAG untuk proses transformasi dan feature building
│ ├── logs/ # Log otomatis dari Airflow
│ └── requirements.txt # Library tambahan untuk Airflow
│
├── etl/
│ ├── fetch_prices.py # Script untuk mengambil data saham (Yahoo Finance)
│ └── build_features.py # Script untuk membuat fitur analisis (moving average, dsb)
│
├── db/
│ └── init.sql # Inisialisasi database & schema (raw, stg, dwh)
│
├── .env # Konfigurasi environment (DB user, password, nama DB)
├── docker-compose.yml # Setup Docker untuk Postgres + Airflow
└── README.md


---

## Alur ELT
1. **Extract (fetch_prices.py)**  
   Mengambil data harga saham dari Yahoo Finance melalui pustaka `yfinance`, kemudian menyimpannya ke tabel:
   - `public.raw_prices`

2. **Transform (build_features.py)**  
   Melakukan pembersihan data dan menambahkan fitur analisis seperti:
   - Moving Average  
   - Daily Return  
   - Price Change Ratio  
   Hasilnya disimpan ke tabel:
   - `public.stg_prices_1h`
   - `public.dwh_features_prices`

3. **Load (DAG Airflow)**  
   Kedua script ELT diatur dan dijalankan otomatis oleh Airflow:
   - `idx_prices_hourly.py` menjalankan proses extract secara berkala (hourly).  
   - `idx_features_hourly.py` menjalankan proses transformasi setelah extract selesai.

---

## Struktur Database
Database `idx` memiliki tiga schema utama:

| Schema | Fungsi | Contoh Tabel |
|--------|---------|--------------|
| raw | Menyimpan data mentah hasil scraping | raw_prices |
| stg | Menyimpan data hasil pembersihan | stg_prices_1h |
| dwh | Menyimpan data akhir siap analisis | dwh_features_prices |

---

## Cara Menjalankan
1. Jalankan Docker:
   ```bash
   docker compose up -d
2. Buka Airflow UI:
    http://localhost:8080
3. Login menggunakan:
    Username: admin
    Password: admin
4. Aktifkan DAG:
    idx_prices_hourly
    idx_features_hourly
    Lihat hasil di DBeaver:
    Host: localhost
    Port: 5432
    User: postgres
    Password: postgres
    Database: idx

Hasil Akhir

Pipeline berhasil menghasilkan data bersih dan siap digunakan untuk analisis.
Data disimpan di PostgreSQL dan dapat digunakan untuk pembuatan dashboard (Power BI) atau analisis lanjutan seperti machine learning.
Airflow secara otomatis menjalankan proses ELT sesuai jadwal yang ditentukan (hourly atau daily).

Teknologi yang Digunakan
    Python 3.12
    Apache Airflow 2.9.2
    PostgreSQL 15
    Docker & Docker Compose
    Pandas
    YFinance
    Psycopg2

Penulis
Hardy Gustino
Gmail :harydgustino815@gmail.com
linkedin :[https://www.linkedin.com/in/hardygustino/]
