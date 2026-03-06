-- Total rides
SELECT COUNT(*) AS total_rides
FROM taxi_trips;

-- Total revenue
SELECT ROUND(SUM(total_amount)::numeric, 2) AS total_revenue
FROM taxi_trips;

-- Average fare
SELECT ROUND(AVG(total_amount)::numeric, 2) AS avg_fare
FROM taxi_trips;

-- Average trip distance
SELECT ROUND(AVG(trip_distance)::numeric, 2) AS avg_trip_distance
FROM taxi_trips;

-- Revenue by payment type
SELECT payment_type,
       COUNT(*) AS rides,
       ROUND(SUM(total_amount)::numeric, 2) AS revenue
FROM taxi_trips
GROUP BY payment_type
ORDER BY revenue DESC;

-- Peak pickup hours
SELECT EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
       COUNT(*) AS total_rides,
       ROUND(SUM(total_amount)::numeric, 2) AS revenue
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY total_rides DESC;

-- Daily revenue trend
SELECT DATE(pickup_datetime) AS trip_date,
       ROUND(SUM(total_amount)::numeric, 2) AS daily_revenue
FROM taxi_trips
GROUP BY trip_date
ORDER BY trip_date;

-- Passenger count distribution
SELECT passenger_count,
       COUNT(*) AS total_rides
FROM taxi_trips
GROUP BY passenger_count
ORDER BY passenger_count;

-- Top 10 longest average-fare distances
SELECT ROUND(trip_distance::numeric, 2) AS trip_distance,
       COUNT(*) AS trip_count,
       ROUND(AVG(total_amount)::numeric, 2) AS avg_fare
FROM taxi_trips
GROUP BY ROUND(trip_distance::numeric, 2)
ORDER BY trip_distance DESC
LIMIT 10;