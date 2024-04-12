-- Insert dummy data for Customers
INSERT INTO Customers (customer_id, name, phone_number, address, city) VALUES
(1, 'John Doe', '123-456-7890', '123 Main St', 'New York'),
(2, 'Jane Smith', '987-654-3210', '456 Elm St', 'Los Angeles');

-- Insert dummy data for Restaurants
INSERT INTO Restaurants (restaurant_id, name, cuisine, address, city, average_rating) VALUES
(1, 'Pizza Palace', 'Italian', '789 Oak St', 'New York', 4.5),
(2, 'Sushi Spot', 'Japanese', '321 Maple Ave', 'Los Angeles', 4.0);

-- Insert dummy data for Couriers
INSERT INTO Couriers (courier_id, name, vehicle_type, city, is_active, average_rating) VALUES
(1, 'Mike Johnson', 'Car', 'New York', TRUE, 4.2),
(2, 'Sarah Lee', 'Bike', 'Los Angeles', TRUE, 4.6);

-- Insert dummy data for Orders
INSERT INTO Orders (order_id, customer_id, restaurant_id, courier_id, order_time, pickup_time, delivery_time, origin_longitude, origin_latitude, destination_longitude, destination_latitude, distance, delivery_fee, payment_method, order_items, order_type, rating, comments) VALUES
(1, 1, 1, 1, '2024-04-12 12:00:00', '2024-04-12 12:15:00', '2024-04-12 12:45:00', -73.987, 40.743, -73.957, 40.774, 5.2, 10.0, 'Credit Card', '{"item": "Pizza", "quantity": 1, "price": 12.99}', 'food', 5, 'Great service!'),
(2, 2, 2, 2, '2024-04-12 13:30:00', '2024-04-12 13:45:00', '2024-04-12 14:15:00', -118.234, 34.052, -118.365, 34.165, 8.5, 15.0, 'Cash', '{"item": "Sushi", "quantity": 2, "price": 24.99}', 'food', 4, 'Delivery was a bit late.');

-- Insert more dummy data as needed
