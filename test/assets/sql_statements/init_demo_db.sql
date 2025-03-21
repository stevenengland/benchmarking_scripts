-- Create a schema for the sales demo system
CREATE USER sales_demo IDENTIFIED BY demo_password;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE ANY VIEW TO sales_demo;
ALTER USER sales_demo QUOTA UNLIMITED ON USERS;

-- Connect to the schema
CONNECT sales_demo/demo_password;

-- Create departments table
CREATE TABLE departments (
    dept_id NUMBER PRIMARY KEY,
    dept_name VARCHAR2(30) NOT NULL
);

-- Create employees table
CREATE TABLE employees (
    emp_id NUMBER PRIMARY KEY,
    name VARCHAR2(50) NOT NULL,
    dept_id NUMBER,
    CONSTRAINT fk_dept FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

-- Create customers table
CREATE TABLE customers (
    cust_id NUMBER PRIMARY KEY,
    name VARCHAR2(50) NOT NULL,
    email VARCHAR2(50)
);

-- Create products table
CREATE TABLE products (
    prod_id NUMBER PRIMARY KEY,
    name VARCHAR2(50) NOT NULL,
    price NUMBER(8,2) NOT NULL
);

-- Create orders table
CREATE TABLE orders (
    order_id NUMBER PRIMARY KEY,
    cust_id NUMBER NOT NULL,
    emp_id NUMBER NOT NULL,
    order_date DATE NOT NULL,
    CONSTRAINT fk_cust FOREIGN KEY (cust_id) REFERENCES customers(cust_id),
    CONSTRAINT fk_emp FOREIGN KEY (emp_id) REFERENCES employees(emp_id)
);

-- Create order_items table
CREATE TABLE order_items (
    order_id NUMBER,
    prod_id NUMBER,
    quantity NUMBER NOT NULL,
    PRIMARY KEY (order_id, prod_id),
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_prod FOREIGN KEY (prod_id) REFERENCES products(prod_id)
);

-- Create sequences
CREATE SEQUENCE dept_seq START WITH 1;
CREATE SEQUENCE emp_seq START WITH 1;
CREATE SEQUENCE cust_seq START WITH 1;
CREATE SEQUENCE prod_seq START WITH 1;
CREATE SEQUENCE order_seq START WITH 1;

-- Insert 20 departments
BEGIN
    FOR i IN 1..20 LOOP
        INSERT INTO departments (dept_id, dept_name)
        VALUES (dept_seq.NEXTVAL, 'Department ' || i);
    END LOOP;
END;
/

-- Insert 50 employees
BEGIN
    FOR i IN 1..50 LOOP
        INSERT INTO employees (emp_id, name, dept_id)
        VALUES (emp_seq.NEXTVAL, 'Employee ' || i, CEIL(DBMS_RANDOM.VALUE(1, 20)));
    END LOOP;
END;
/

-- Insert 100 customers
BEGIN
    FOR i IN 1..100 LOOP
        INSERT INTO customers (cust_id, name, email)
        VALUES (cust_seq.NEXTVAL, 'Customer ' || i, 'customer' || i || '@example.com');
    END LOOP;
END;
/

-- Insert 1000 products
BEGIN
    FOR i IN 1..1000 LOOP
        INSERT INTO products (prod_id, name, price)
        VALUES (prod_seq.NEXTVAL, 'Product ' || i, ROUND(DBMS_RANDOM.VALUE(10, 1000), 2));
    END LOOP;
END;
/

-- Insert sample orders and order items (200 orders)
BEGIN
    FOR i IN 1..200 LOOP
        -- Insert order
        INSERT INTO orders (order_id, cust_id, emp_id, order_date)
        VALUES (
            order_seq.NEXTVAL,
            CEIL(DBMS_RANDOM.VALUE(1, 100)),    -- Random customer
            CEIL(DBMS_RANDOM.VALUE(1, 50)),     -- Random employee
            SYSDATE - CEIL(DBMS_RANDOM.VALUE(1, 365)) -- Random date in past year
        );
        
        -- Insert 1-5 order items for this order, ensuring no duplicates
        DECLARE
            v_order_id NUMBER := order_seq.CURRVAL;
            v_num_items NUMBER := CEIL(DBMS_RANDOM.VALUE(1, 5));
            TYPE product_array IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
            v_products product_array;
            v_product_count NUMBER := 0;
            v_new_product NUMBER;
            v_duplicate BOOLEAN;
        BEGIN
            -- Keep adding products until we have the desired number
            WHILE v_product_count < v_num_items LOOP
                v_new_product := CEIL(DBMS_RANDOM.VALUE(1, 1000));
                
                -- Check if this product is already in our array
                v_duplicate := FALSE;
                FOR j IN 1..v_product_count LOOP
                    IF v_products(j) = v_new_product THEN
                        v_duplicate := TRUE;
                        EXIT;
                    END IF;
                END LOOP;
                
                -- If not a duplicate, add it to our order
                IF NOT v_duplicate THEN
                    v_product_count := v_product_count + 1;
                    v_products(v_product_count) := v_new_product;
                    
                    INSERT INTO order_items (order_id, prod_id, quantity)
                    VALUES (
                        v_order_id,
                        v_new_product,
                        CEIL(DBMS_RANDOM.VALUE(1, 10))
                    );
                END IF;
            END LOOP;
        END;
    END LOOP;
END;
/

-- Create a simple view for reporting
CREATE VIEW sales_summary AS
SELECT 
    o.order_id,
    c.name AS customer,
    e.name AS employee,
    d.dept_name,
    o.order_date,
    SUM(oi.quantity * p.price) AS total_amount
FROM 
    orders o
    JOIN customers c ON o.cust_id = c.cust_id
    JOIN employees e ON o.emp_id = e.emp_id
    JOIN departments d ON e.dept_id = d.dept_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.prod_id = p.prod_id
GROUP BY 
    o.order_id, c.name, e.name, d.dept_name, o.order_date;

COMMIT;