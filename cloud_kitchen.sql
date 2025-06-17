-- 1️⃣ Data Cleaning (ETL)
-- A. Create a Cleaned Table
CREATE OR REPLACE TABLE `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_cleaned` AS
SELECT
  SAFE_CAST(Restaurant_ID AS STRING) AS Restaurant_ID,
  TRIM(Restaurant_Name) AS Restaurant_Name,
  TRIM(Subzone) AS Subzone,
  TRIM(City) AS City,
  SAFE_CAST(Order_ID AS STRING) AS Order_ID,
  -- Only parse if Time_of_Order looks like a valid date string
  PARSE_TIMESTAMP('%I:%M %p, %B %d %Y', Time_of_Order) AS Order_Timestamp
FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders`
WHERE 
  Restaurant_ID IS NOT NULL
  AND Order_ID IS NOT NULL
  AND Restaurant_Name IS NOT NULL
  AND REGEXP_CONTAINS(Time_of_Order, r'^\d{1,2}:\d{2} [AP]M, [A-Za-z]+ \d{2} \d{4}$');

  -- 2️⃣ Feature Engineering
  CREATE OR REPLACE TABLE `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_features` AS
SELECT
  *,
  EXTRACT(DATE FROM Order_Timestamp) AS Order_Date,
  EXTRACT(HOUR FROM Order_Timestamp) AS Order_Hour,
  EXTRACT(DAYOFWEEK FROM Order_Timestamp) AS Order_Weekday,
  CASE WHEN EXTRACT(DAYOFWEEK FROM Order_Timestamp) IN (1,7) THEN 1 ELSE 0 END AS Is_Weekend
FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_cleaned`;

-- 3️⃣ Exploratory Analysis
-- Total Orders by City
CREATE OR REPLACE TABLE `glass-haven-462722-u4.cloud_kitchen_ds.city_order_counts` AS
SELECT City, COUNT(*) AS Order_Count
FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_features`
GROUP BY City
ORDER BY Order_Count DESC;

-- Orders by Restaurant
CREATE OR REPLACE TABLE `glass-haven-462722-u4.cloud_kitchen_ds.restaurant_order_counts` AS
SELECT Restaurant_Name, COUNT(*) AS Orders
FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_features`
GROUP BY Restaurant_Name
ORDER BY Orders DESC;

-- Orders by Hour
CREATE OR REPLACE TABLE `glass-haven-462722-u4.cloud_kitchen_ds.hourly_orders` AS
SELECT Order_Hour, COUNT(*) AS Orders
FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_features`
GROUP BY Order_Hour
ORDER BY Order_Hour;

-- 4️⃣ Machine Learning: Predict Busy Hour
-- Add ML Label
CREATE OR REPLACE TABLE `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_ml` AS
SELECT *,
  CASE WHEN Order_Hour BETWEEN 18 AND 22 THEN 1 ELSE 0 END AS Is_Busy_Hour
FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_features`;

-- Train Logistic Regression
CREATE OR REPLACE MODEL `glass-haven-462722-u4.cloud_kitchen_ds.busy_hour_classifier`
OPTIONS(
  model_type='logistic_reg',
  input_label_cols=['Is_Busy_Hour']
) AS
SELECT
  Order_Hour,
  Order_Weekday,
  Is_Weekend,
  Is_Busy_Hour
FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_ml`;

-- Predict with Model
CREATE OR REPLACE TABLE `glass-haven-462722-u4.cloud_kitchen_ds.busy_hour_predictions` AS
SELECT 
  *
FROM ML.PREDICT(
  MODEL `glass-haven-462722-u4.cloud_kitchen_ds.busy_hour_classifier`,
  (
    SELECT
      Order_Hour,
      Order_Weekday,
      Is_Weekend
    FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_ml`
  )
);

-- 5️⃣ View Results

SELECT * FROM `glass-haven-462722-u4.cloud_kitchen_ds.busy_hour_predictions` LIMIT 100;

-- Optional: Advanced Suggestions
-- 1 Trend by Date
SELECT Order_Date, COUNT(*) AS Orders
FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_features`
GROUP BY Order_Date ORDER BY Order_Date;

-- 2 Top 10 Restaurants
SELECT Restaurant_Name, COUNT(*) AS Total_Orders
FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_features`
GROUP BY Restaurant_Name
ORDER BY Total_Orders DESC
LIMIT 10;

-- 3 Evaluate ML Model
SELECT *
FROM ML.EVALUATE(
  MODEL `glass-haven-462722-u4.cloud_kitchen_ds.busy_hour_classifier`,
  (
    SELECT Order_Hour, Order_Weekday, Is_Weekend, Is_Busy_Hour
    FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_ml`
  )
);

-- 4 Explain ML Model
SELECT *
FROM ML.EXPLAIN_PREDICT(
  MODEL `glass-haven-462722-u4.cloud_kitchen_ds.busy_hour_classifier`,
  (
    SELECT Order_Hour, Order_Weekday, Is_Weekend
    FROM `glass-haven-462722-u4.cloud_kitchen_ds.food_orders_ml`
    LIMIT 100
  )
);


