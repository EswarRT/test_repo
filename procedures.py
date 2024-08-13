import snowflake.connector
# import sqlite3

# Establish connection to Snowflake
conn = snowflake.connector.connect(
    user='ESWARMANIKANTA',
    password='Eswar@7185',
    account='hd90197.europe-west4.gcp',
    warehouse='COMPUTE_WH',
    database='PUBLIC',
    schema='PUBLIC'
)
# null.HD90197
# Create a cursor object
cur = conn.cursor()

# Function to execute SQL commands
def execute_sql(command):
    try:
        cur.execute(command)
        print(f"Executed: {command}")
    except Exception as e:
        print(f"Error executing {command}: {e}")

# Step 1: Create the CUSTOMERS table
create_customers_table = """
CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID NUMBER PRIMARY KEY,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    PHONE_NUMBER STRING,
    JOIN_DATE DATE
);
"""
execute_sql(create_customers_table)

# Step 2: Load data into the CUSTOMERS table
insert_customers_data = """
INSERT INTO CUSTOMERS (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, JOIN_DATE) VALUES 
(1, 'Alice', 'Johnson', 'alice.johnson@example.com', '123-456-7890', '2021-05-10'),
(2, 'Bob', 'Smith', 'bob.smith@example.com', '234-567-8901', '2020-08-15'),
(3, 'Carol', 'Williams', 'carol.williams@example.com', '345-678-9012', '2019-11-20'),
(4, 'David', 'Brown', 'david.brown@example.com', '456-789-0123', '2022-01-25');
"""
execute_sql(insert_customers_data)

# Step 3: Create a stored procedure for the CUSTOMERS table
create_update_customer_email_proc = """
CREATE OR REPLACE PROCEDURE update_customer_email(p_customer_id NUMBER, p_new_email STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    var sql_command = `UPDATE CUSTOMERS SET EMAIL = '` + p_new_email + `' WHERE CUSTOMER_ID = ` + p_customer_id;
    var statement1 = snowflake.createStatement({sqlText: sql_command});
    statement1.execute();
    return 'Customer email updated successfully';
} catch (err) {
    return 'Error: ' + err;
}
$$;
"""
execute_sql(create_update_customer_email_proc)
# def call_update_customer_email(p_customer_id, p_new_email):
#     try:
#         cur.callproc('update_customer_email', [p_customer_id, p_new_email])
#         print(f"Customer email updated successfully for CUSTOMER_ID = {p_customer_id}")
#     except Exception as e:
#         print(f"Error updating customer email: {e}")

# # Example: Update email for customer with CUSTOMER_ID 2
# call_update_customer_email(2, 'new.email@example.com')

# # Verify the changes
# def query_customers():
#     try:
#         cur.execute("SELECT * FROM CUSTOMERS")
#         for row in cur:
#             print(row)
#     except Exception as e:
#         print(f"Error querying CUSTOMERS table: {e}")

# query_customers()

# Close the cursor and connection
cur.close()
conn.close()
# -----------------------------------------------------------------------------------------------------------------------------------
# # Step 4: Create the ORDERS table
# create_orders_table = """
# CREATE OR REPLACE TABLE ORDERS (
#     ORDER_ID NUMBER PRIMARY KEY,
#     CUSTOMER_ID NUMBER,
#     ORDER_DATE DATE,
#     TOTAL_AMOUNT NUMBER,
#     STATUS STRING,
#     FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID)
# );
# """
# execute_sql(create_orders_table)

# # Step 5: Load data into the ORDERS table
# insert_orders_data = """
# INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, TOTAL_AMOUNT, STATUS) VALUES 
# (1001, 1, '2023-06-15', 250.00, 'Shipped'),
# (1002, 2, '2023-07-01', 150.00, 'Pending'),
# (1003, 3, '2023-07-10', 300.00, 'Delivered'),
# (1004, 4, '2023-07-15', 100.00, 'Cancelled');
# """
# execute_sql(insert_orders_data)

# # Step 6: Create a stored procedure for the ORDERS table
# create_update_order_status_proc = """
# CREATE OR REPLACE PROCEDURE update_order_status(p_order_id NUMBER, p_new_status STRING)
# RETURNS STRING
# LANGUAGE JAVASCRIPT
# AS
# $$
# try {
#     var sql_command = `UPDATE ORDERS SET STATUS = '` + p_new_status + `' WHERE ORDER_ID = ` + p_order_id;
#     var statement1 = snowflake.createStatement({sqlText: sql_command});
#     statement1.execute();
#     return 'Order status updated successfully';
# } catch (err) {
#     return 'Error: ' + err;
# }
# $$;
# """
# execute_sql(create_update_order_status_proc)

# Close the cursor and connection
# cur.close()
# conn.close()
