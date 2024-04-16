create schema airlines;

CREATE TABLE airlines.airports_dim (
    airport_id BIGINT,
    city VARCHAR(100),
    state VARCHAR(100),
    name VARCHAR(200)
);

COPY airlines.airports_dim
FROM 's3://airline-landing-zn/dim/airports.csv'
IAM_ROLE 'arn:aws:iam::891377180984:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T160936'
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-2';

--------------

CREATE TABLE airlines.daily_flights_fact (
    carrier VARCHAR(10),
    dep_airport VARCHAR(200),
    arr_airport VARCHAR(200),
    dep_city VARCHAR(100),
    arr_city VARCHAR(100),
    dep_state VARCHAR(100),
    arr_state VARCHAR(100),
    dep_delay BIGINT,
    arr_delay BIGINT
);