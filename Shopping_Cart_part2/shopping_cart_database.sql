-- Create the database
CREATE DATABASE IF NOT EXISTS shopping_cart;
USE shopping_cart;

-- Create the items table
CREATE TABLE IF NOT EXISTS items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Item VARCHAR(255) NOT NULL,
    Cost DECIMAL(10, 2) NOT NULL,
    Quantity INT NOT NULL
);

-- Create the orders table
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    total_amount DECIMAL(10, 2) NOT NULL,
    delivery_charge DECIMAL(10, 2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create the order_items table
CREATE TABLE IF NOT EXISTS order_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    item_id INT,
    quantity INT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (item_id) REFERENCES items(id)
);

-- Insert some sample items
INSERT INTO items (Item,Cost,Quantity) VALUES
('Biscuit', 10.00, 5),
('Cereals', 90.00, 10),
('Chicken', 100.0, 20);
UPDATE Items
SET Quantity = 20
WHERE id = 3;

select * from items;