-- Create the rideapp database
CREATE DATABASE IF NOT EXISTS rideapp;

-- Switch to rideapp database
USE rideapp;

-- Create the Riders table
CREATE TABLE IF NOT EXISTS Customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255),
    phone_number VARCHAR(20),
    address VARCHAR(100)
);

-- Create the Drivers table
CREATE TABLE IF NOT EXISTS Drivers (
    driver_id INT PRIMARY KEY,
    name VARCHAR(255),
    license_plate VARCHAR(20),
    car_model VARCHAR(100),
    city VARCHAR(100),
    is_active BOOLEAN,
    average_rating FLOAT
);

-- Create the Trips table
CREATE TABLE IF NOT EXISTS Trips (
    trip_id INT PRIMARY KEY,
    driver_id INT,
    customer_id INT,
    request_time TIMESTAMP,
    pickup_time TIMESTAMP,
    dropoff_time TIMESTAMP,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    dropoff_longitude FLOAT,
    dropoff_latitude FLOAT,
    distance FLOAT,
    fare_amount FLOAT,
    payment_method VARCHAR(50),
    rating INT,
    comments TEXT,
    FOREIGN KEY (driver_id) REFERENCES Drivers(driver_id),
    FOREIGN KEY (customer_id) REFERENCES Riders(customer_id)
);

-- Create the Locations table
CREATE TABLE IF NOT EXISTS Locations (
    location_id INT PRIMARY KEY,
    address VARCHAR(255),
    longitude FLOAT,
    latitude FLOAT,
    location_type VARCHAR(20) 
);
