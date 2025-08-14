import pandas as pd
import mysql.connector

# 1. Baca CSV
df = pd.read_csv("studi kasus/coffee sales/Data_Coffee.csv")

# 2. Ubah ke datetime dan buang yang gagal
df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
df = df.dropna(subset=['date'])

# 3. Ganti NaN lain jadi None
df = df.where(pd.notnull(df), None)

# 4. Koneksi ke MySQL
conn = mysql.connector.connect(
    host='localhost',
    port=3306,
    user='root',
    password='rooter123',
    database='datapenjualan'
)
cursor = conn.cursor()

# 5. Buat tabel
cursor.execute("""
    CREATE TABLE IF NOT EXISTS penjualan (
        id INT AUTO_INCREMENT PRIMARY KEY,
        tanggal DATE,
        lama_buka INT,
        tipe_pembayaran VARCHAR(50),
        harga DECIMAL(10,2),
        nama_minuman VARCHAR(100),
        hari_terjual VARCHAR(20),
        bulan_terjual VARCHAR(20)
    )
""")

# 6. INSERT ke database
for _, row in df.iterrows():
    tanggal_obj = row['date'].date()
    
    cursor.execute("""
        INSERT INTO penjualan (
            tanggal, lama_buka, tipe_pembayaran, harga,
            nama_minuman, hari_terjual, bulan_terjual
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        tanggal_obj,
        row['hour_of_day'],
        row['cash_type'],
        row['money'],
        row['coffee_name'],
        row['Weekday'],
        row['Month_name']
    ))

# 7. Selesai
conn.commit()
cursor.close()
conn.close()
print("Data berhasil dikirim ke MySQL.")
