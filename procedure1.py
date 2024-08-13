import snowflake.connector

# Establish connection to Snowflake
conn = snowflake.connector.connect(
    user='ESWARMANIKANTA',
    password='Eswar@7185',
    account='hd90197.europe-west4.gcp',
    warehouse='COMPUTE_WH',
    database='PUBLIC',
    schema='PUBLIC'
)

# Create a cursor object
cur = conn.cursor()

# Function to execute SQL commands
def execute_sql(command):
    try:
        cur.execute(command)
        print(f"Executed: {command}")
    except Exception as e:
        print(f"Error executing {command}: {e}")

# Step 1: Create the CUSTOMERS table in Snowflake
create_customers_table = """
CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID FLOAT PRIMARY KEY,
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

# Step 3: Create a stored procedure for updating customer email
create_update_customer_email_proc = """
CREATE OR REPLACE PROCEDURE update_customer_email(p_customer_id IN NUMBER, p_new_email IN VARCHAR2) AS
BEGIN
    UPDATE CUSTOMERS
    SET EMAIL = p_new_email
    WHERE CUSTOMER_ID = p_customer_id;
 
    COMMIT;
END;

 
-- Example usage of the stored procedure
BEGIN
    update_customer_email(2, 'new.email@example.com');
END;

"""
execute_sql(create_update_customer_email_proc)

# Step 4: Example usage of the stored procedure
def call_update_customer_email(p_customer_id, p_new_email):
    try:
        cur.callproc('update_customer_email', [p_customer_id, p_new_email])
        print(f"Customer email updated successfully for CUSTOMER_ID = {p_customer_id}")
    except Exception as e:
        print(f"Error updating customer email: {e}")

# Example: Update email for customer with CUSTOMER_ID 2
call_update_customer_email(2, 'new.email@example.com')

# Verify the changes
def query_customers():
    try:
        cur.execute("SELECT * FROM CUSTOMERS")
        for row in cur:
            print(row)
    except Exception as e:
        print(f"Error querying CUSTOMERS table: {e}")

query_customers()

# Close the cursor and connection
cur.close()
conn.close()
