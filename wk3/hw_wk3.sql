-- Question 1. What is count for fhv vehicles data for year 2019?
SELECT COUNT(*) as trips
FROM `de-camp-practice.trips_data_all.fhv_tripdata`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2020-01-01';
-- Answer: 42084899

-- Question 2. How many distinct dispatching_base_num we have in fhv for 2019?
SELECT COUNT(*)
FROM
    (SELECT DISTINCT dispatching_base_num
    FROM `de-camp-practice.trips_data_all.fhv_tripdata`
    WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2020-01-01');
-- Answer: 792

-- Question 3. Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num

-- Question 4. What is the count, estimated and actual data processed for query which counts trip
-- between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279?
SELECT COUNT(*) as trips
FROM `de-camp-practice.trips_data_all.fhv_tripdata`
WHERE
    DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND
    dispatching_base_num IN ("B00987", "B02060", "B02279");
-- This query will process 400.1 MiB when run.
-- Query complete (0.6 sec elapsed, 400.1 MB processed)
-- Count:26647

