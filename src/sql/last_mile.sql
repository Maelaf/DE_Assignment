-- Create the deliveries database
CREATE DATABASE IF NOT EXISTS deliveries;

-- Switch to deliveries database
USE deliveries;

-- Create the Customers table
CREATE TABLE IF NOT EXISTS Customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255),
    phone_number VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100)
);

-- Create the Stores table
CREATE TABLE IF NOT EXISTS Stores (
    store_id INT PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    address VARCHAR(255),
    city VARCHAR(100),
    average_rating FLOAT
);

-- Create the Couriers table
CREATE TABLE IF NOT EXISTS Couriers (
    courier_id INT PRIMARY KEY,
    name VARCHAR(255),
    vehicle_type VARCHAR(100),
    city VARCHAR(100),
    is_active BOOLEAN,
    average_rating FLOAT
);

-- Create the Locations table
CREATE TABLE IF NOT EXISTS Locations (
    location_id INT PRIMARY KEY,
    address VARCHAR(255),
    longitude FLOAT,
    latitude FLOAT,
    location_type VARCHAR(20) -- "origin" or "destination"
);

-- Create the Deliveries table
CREATE TABLE IF NOT EXISTS Deliveries (
    delivery_id INT PRIMARY KEY,
    customer_id INT,
    store_id INT,
    courier_id INT,
    order_time TIMESTAMP,
    pickup_time TIMESTAMP,
    delivery_time TIMESTAMP,
    origin_longitude FLOAT,
    origin_latitude FLOAT,
    destination_longitude FLOAT,
    destination_latitude FLOAT,
    distance FLOAT,
    delivery_fee FLOAT,
    payment_method VARCHAR(50),
    order_items JSON,
    rating INT,
    comments TEXT,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES Stores(store_id),
    FOREIGN KEY (courier_id) REFERENCES Couriers(courier_id)
);

-- Create the Time table
CREATE TABLE IF NOT EXISTS Time (
    date DATE PRIMARY KEY,
    hour INT,
    day_of_week VARCHAR(20),
    week_of_year INT,
    month INT,
    year INT
);
