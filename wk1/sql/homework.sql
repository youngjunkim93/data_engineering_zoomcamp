-- Question 3. How many taxi trips were there on January 15?
SELECT COUNT(*) FROM yellow_taxi_data 
    WHERE DATE(tpep_pickup_datetime)=DATE('2021-01-15');

-- Question 4-1. Find the largest tip for each day
SELECT DATE(tpep_pickup_datetime) AS trip_date, MAX(tip_amount) AS max_tip_amount
    FROM yellow_taxi_data
    WHERE DATE_PART('year', CAST(tpep_pickup_datetime AS DATE)) = '2021' 
        AND DATE_PART('month', CAST(tpep_pickup_datetime AS DATE))= '01'
    GROUP BY trip_date
    ORDER BY trip_date ASC;

-- Question 4-2. Find the day of largest tip in January
SELECT DATE(tpep_pickup_datetime) AS trip_date, MAX(tip_amount) AS max_tip_amount 
    FROM yellow_taxi_data 
    WHERE DATE_PART('year', CAST(tpep_pickup_datetime AS DATE)) = '2021' 
        AND DATE_PART('month', CAST(tpep_pickup_datetime AS DATE))= '01'
    GROUP BY trip_date 
    ORDER BY max_tip_amount DESC
    LIMIT 1;

-- Question 5. Find the most popular destination for passengers picked up in central park on January 14?
SELECT z_do."Zone" AS dropoff_zone, COUNT(*) AS trip_count 
    FROM yellow_taxi_data AS t
    INNER JOIN zone_data AS z_pu
        ON z_pu."LocationID" = t."PULocationID"
    INNER JOIN zone_data AS z_do
        ON z_do."LocationID" = t."DOLocationID"
    WHERE z_pu."Zone" = 'Central Park' AND DATE(t."tpep_pickup_datetime") = DATE('2021-01-14') 
    GROUP BY z_do."LocationID"
    ORDER BY trip_count DESC
    LIMIT 1;

-- Question 6. Pickup-Dropoff pair with the largest price for a ride (based on total_amount)
-- Here, I omitted the ride with unknown pickup or dropoff ID (possibly beyond NYC taxi boundary)
SELECT z_pu."Zone" AS pickup_zone, z_do."Zone" AS dropoff_zone, 
		ROUND(AVG(total_amount)::numeric, 2) AS average_fare
    FROM yellow_taxi_data AS t
    INNER JOIN zone_data AS z_pu
        ON z_pu."LocationID" = t."PULocationID"
    INNER JOIN zone_data AS z_do
        ON z_do."LocationID" = t."DOLocationID"
    WHERE z_do."LocationID" NOT IN (264, 265) AND z_pu."LocationID" NOT IN (264, 265)
    GROUP BY z_pu."LocationID", z_do."LocationID"
    ORDER BY average_fare DESC
    LIMIT 1;