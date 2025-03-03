-- 1. Mengambil rating cabang dan nama cabang dari tabel kantor cabang, diurutkan berdasarkan rating terbesar
SELECT 
  branch_id, 
  branch_name,
  rating AS rating_cabang 
FROM `Dataset_PBI_KimiaFarma.kf_kantor_cabang`
ORDER BY rating_cabang DESC;

-- 2. Mengambil nama customer dari tabel transaksi
SELECT 
  transaction_id, 
  customer_name 
FROM `Dataset_PBI_KimiaFarma.kf_final_transaction`;

-- 3. Mengambil kode produk dari tabel transaksi
SELECT 
  transaction_id, 
  product_id 
FROM `Dataset_PBI_KimiaFarma.kf_final_transaction`;

-- 4. Mengambil nama obat berdasarkan product_id dari tabel produk
SELECT 
  p.product_id, 
  COALESCE(p.product_name, i.product_name) AS product_name
FROM `Dataset_PBI_KimiaFarma.kf_product` p
LEFT JOIN `Dataset_PBI_KimiaFarma.kf_inventory` i 
  ON p.product_id = i.product_id;

-- 5. Mengambil harga obat dari tabel transaksi
SELECT 
  transaction_id, 
  price AS actual_price 
FROM `Dataset_PBI_KimiaFarma.kf_final_transaction`;

-- 6. Mengambil persentase diskon dari tabel transaksi
SELECT 
  transaction_id, 
  discount_percentage 
FROM `Dataset_PBI_KimiaFarma.kf_final_transaction`;

-- 7. Menentukan persentase gross laba berdasarkan harga obat
SELECT 
  transaction_id, 
  price,
  CASE 
    WHEN price <= 50000 THEN 0.10
    WHEN price > 50000 AND price <= 100000 THEN 0.15
    WHEN price > 100000 AND price <= 300000 THEN 0.20
    WHEN price > 300000 AND price <= 500000 THEN 0.25
    ELSE 0.30
  END AS persentase_gross_laba
FROM `Dataset_PBI_KimiaFarma.kf_final_transaction`;

-- 8. Menghitung nett sales (harga setelah diskon)
SELECT 
  transaction_id, 
  price * (1 - discount_percentage) AS nett_sales 
FROM `Dataset_PBI_KimiaFarma.kf_final_transaction`;

-- 9. Menghitung nett profit (keuntungan yang diperoleh Kimia Farma)
SELECT 
  transaction_id, 
  (price * (1 - discount_percentage)) * 
  CASE 
    WHEN price <= 50000 THEN 0.10
    WHEN price > 50000 AND price <= 100000 THEN 0.15
    WHEN price > 100000 AND price <= 300000 THEN 0.20
    WHEN price > 300000 AND price <= 500000 THEN 0.25
    ELSE 0.30
  END AS nett_profit
FROM `Dataset_PBI_KimiaFarma.kf_final_transaction`;

-- 10. Mengambil rating transaksi dari tabel transaksi
SELECT 
  transaction_id, 
  rating AS rating_transaksi 
FROM `Dataset_PBI_KimiaFarma.kf_final_transaction`;


SELECT * 
FROM `Dataset_PBI_KimiaFarma.kf_performance_analysis`
ORDER BY RAND()
LIMIT 5;


CREATE OR REPLACE TABLE `Dataset_PBI_KimiaFarma.kf_performance_analysis`
PARTITION BY DATE(date)
CLUSTER BY branch_id, product_id
AS
WITH profit_calculation AS (
  SELECT
    t.transaction_id,
    t.date,
    EXTRACT(YEAR FROM t.date) AS year, 
    t.branch_id,
    b.branch_name,
    b.kota,
    b.provinsi,
    b.rating AS rating_cabang,
    t.customer_name,
    t.product_id,
    COALESCE(p.product_name, i.product_name) AS product_name,
    t.price AS actual_price,
    t.discount_percentage,
    
    -- Persentase laba berdasarkan harga
    CASE
      WHEN t.price <= 50000 THEN 0.10
      WHEN t.price <= 100000 THEN 0.15
      WHEN t.price <= 300000 THEN 0.20
      WHEN t.price <= 500000 THEN 0.25
      ELSE 0.30
    END AS persentase_gross_laba,

    -- Nett sales setelah diskon
    t.price * (1 - t.discount_percentage) AS nett_sales,

    -- Nett profit (keuntungan bersih)
    (t.price * (1 - t.discount_percentage)) * 
    CASE
      WHEN t.price <= 50000 THEN 0.10
      WHEN t.price <= 100000 THEN 0.15
      WHEN t.price <= 300000 THEN 0.20
      WHEN t.price <= 500000 THEN 0.25
      ELSE 0.30
    END AS nett_profit,

    t.rating AS rating_transaksi

  FROM `Dataset_PBI_KimiaFarma.kf_final_transaction` t
  JOIN `Dataset_PBI_KimiaFarma.kf_kantor_cabang` b
    ON t.branch_id = b.branch_id
  LEFT JOIN `Dataset_PBI_KimiaFarma.kf_product` p
    ON t.product_id = p.product_id
  LEFT JOIN `Dataset_PBI_KimiaFarma.kf_inventory` i
    ON t.product_id = i.product_id
),

-- Perhitungan total transaksi & total nett sales per cabang & provinsi
transactions_summary AS (
  SELECT 
    branch_id,
    branch_name,
    provinsi,
    COUNT(transaction_id) AS total_transaksi,
    SUM(nett_sales) AS total_nett_sales
  FROM profit_calculation
  GROUP BY branch_id, branch_name, provinsi
),

-- Top 5 cabang dengan rating tertinggi tetapi rating transaksi terendah
low_transaction_high_rating AS (
  SELECT 
    branch_id,
    branch_name,
    provinsi,
    rating_cabang,
    AVG(rating_transaksi) AS avg_rating_transaksi
  FROM profit_calculation
  GROUP BY branch_id, branch_name, provinsi, rating_cabang
  ORDER BY rating_cabang DESC, avg_rating_transaksi ASC
  LIMIT 5
),

-- Total profit per provinsi untuk Geo Map
profit_by_province AS (
  SELECT 
    provinsi,
    SUM(nett_profit) AS total_profit
  FROM profit_calculation
  GROUP BY provinsi
)

SELECT
  pc.*,
  ts.total_transaksi,
  ts.total_nett_sales,
  lth.rating_cabang AS high_branch_rating,
  lth.avg_rating_transaksi AS low_transaction_rating,
  pbp.total_profit
FROM profit_calculation pc
LEFT JOIN transactions_summary ts
  ON pc.branch_id = ts.branch_id
LEFT JOIN low_transaction_high_rating lth
  ON pc.branch_id = lth.branch_id
LEFT JOIN profit_by_province pbp
  ON pc.provinsi = pbp.provinsi
ORDER BY pc.date DESC;
