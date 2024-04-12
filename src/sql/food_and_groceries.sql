-- Create the orders database
CREATE DATABASE IF NOT EXISTS groceries;

-- Switch to orders database
USE groceries;

-- Create the Customers table
CREATE TABLE IF NOT EXISTS Customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255),
    phone_number VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100)
);

-- Create the Restaurants table
CREATE TABLE IF NOT EXISTS Restaurants (
    restaurant_id INT PRIMARY KEY,
    name VARCHAR(255),
    cuisine VARCHAR(100),
    address VARCHAR(255),
    city VARCHAR(100),
    average_rating FLOAT
);

-- Create the GroceryStores table
CREATE TABLE IF NOT EXISTS GroceryStores (
    grocery_store_id INT PRIMARY KEY,
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

-- Create the Orders table
CREATE TABLE IF NOT EXISTS Orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    restaurant_id INT,
    grocery_store_id INT,
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
    order_type VARCHAR(20), -- "food" or "grocery"
    rating INT,
    comments TEXT,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (restaurant_id) REFERENCES Restaurants(restaurant_id),
    FOREIGN KEY (grocery_store_id) REFERENCES GroceryStores(grocery_store_id),
    FOREIGN KEY (courier_id) REFERENCES Couriers(courier_id)
);
