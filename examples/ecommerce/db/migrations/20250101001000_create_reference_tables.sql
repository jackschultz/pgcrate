-- Reference tables for order statuses, shipping methods, and countries

CREATE TABLE order_statuses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    is_terminal BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE shipping_methods (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    base_cost DECIMAL(10,2) NOT NULL,
    estimated_days_min INTEGER NOT NULL,
    estimated_days_max INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE countries (
    code CHAR(2) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    shipping_available BOOLEAN NOT NULL DEFAULT TRUE
);

---- down
DROP TABLE IF EXISTS countries;
DROP TABLE IF EXISTS shipping_methods;
DROP TABLE IF EXISTS order_statuses;
