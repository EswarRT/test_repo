import re
import snowflake.connector

# Function to convert Oracle PL/SQL to Snowflake JavaScript stored procedure
def convert_oracle_to_snowflake(oracle_procedure):
    # Define a simple mapping of Oracle to Snowflake data types and constructs
    type_mapping = {
        'NUMBER': 'FLOAT',
        'VARCHAR2': 'STRING',
        'DATE': 'DATE'
    }
    
    # Extract the procedure name and parameters
    proc_name_match = re.search(r'PROCEDURE\s+(\w+)\s*(\((.*?)\))?', oracle_procedure, re.DOTALL)
    if not proc_name_match:
        raise ValueError("The provided Oracle procedure does not match the expected format.")
    
    proc_name = proc_name_match.group(1)
    parameters = proc_name_match.group(3) or ''
    
    # Convert parameters
    param_list = []
    if parameters:
        for param in parameters.split(','):
            param = param.strip()
            param_parts = param.split()
            param_name = param_parts[0]
            param_type = param_parts[-1]  # Take the last part as type to handle cases with missing mode
            param_type = type_mapping.get(param_type.upper(), param_type)
            param_list.append(f'{param_name} {param_type}')
    snowflake_parameters = ', '.join(param_list)
    
    # Extract the body of the procedure
    body_start = oracle_procedure.index("BEGIN") + len("BEGIN")
    body_end = oracle_procedure.rindex("END")  # Use rindex to find the last occurrence of "END"
    procedure_body = oracle_procedure[body_start:body_end].strip()
    
    # Replace Oracle SQL functions with Snowflake SQL equivalents
    procedure_body = procedure_body.replace("SYSDATE", "CURRENT_TIMESTAMP")
    procedure_body = re.sub(r'DBMS_OUTPUT\.PUT_LINE\((.*?)\);', r'result += \1 + "\\n";', procedure_body)  # Replace DBMS_OUTPUT.PUT_LINE with result

    # Remove COMMIT statements (Snowflake auto-commits)
    procedure_body = re.sub(r'\bCOMMIT\b;', '', procedure_body, flags=re.IGNORECASE)

    # Handle PL/SQL control structures and translate them into JavaScript control structures
    procedure_body = re.sub(r'\bIF\b', 'if', procedure_body, flags=re.IGNORECASE)
    procedure_body = re.sub(r'\bTHEN\b', '{', procedure_body, flags=re.IGNORECASE)
    procedure_body = re.sub(r'\bELSIF\b', '} else if', procedure_body, flags=re.IGNORECASE)
    procedure_body = re.sub(r'\bELSE\b', '} else {', procedure_body, flags=re.IGNORECASE)
    procedure_body = re.sub(r'\bEND IF\b', '}', procedure_body, flags=re.IGNORECASE)
    procedure_body = re.sub(r'\bLOOP\b', '{', procedure_body, flags=re.IGNORECASE)
    procedure_body = re.sub(r'\bEND LOOP\b', '}', procedure_body, flags=re.IGNORECASE)
    
    # Split the body into individual statements for proper handling in JavaScript
    statements = procedure_body.split(';')
    statements = [stmt.strip() for stmt in statements if stmt.strip()]

    # Generate Snowflake stored procedure in JavaScript
    snowflake_procedure = f"""
CREATE OR REPLACE PROCEDURE {proc_name}({snowflake_parameters})
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var result = '';
try {{
    var sql_command;
    var statement;

    // Define helper function to execute SQL commands
    function executeSQL(sql_command) {{
        statement = snowflake.createStatement({{sqlText: sql_command}});
        return statement.execute();
    }}

    // Procedure body statements
    {"".join([f'executeSQL(`{stmt}`);' for stmt in statements])}

    result += '{proc_name} executed successfully';
}} catch (err) {{
    result = 'An error occurred: ' + err.message;
}}
return result;
$$;
"""
    return snowflake_procedure

# Example Oracle procedure (truncated for brevity)
oracle_procedure = """
CREATE OR REPLACE PROCEDURE integrate_products_data IS
    CURSOR products_cursor IS
        SELECT product_id, product_name, category, price, stock_quantity
        FROM products_staging;

    v_product_id products_staging.product_id%TYPE;
    v_product_name products_staging.product_name%TYPE;
    v_category products_staging.category%TYPE;
    v_price products_staging.price%TYPE;
    v_stock_quantity products_staging.stock_quantity%TYPE;

    v_product_exists NUMBER;

    ex_invalid_data EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_invalid_data, -20001);

    PROCEDURE validate_data IS
    BEGIN
        -- Validate product data
        IF v_product_id IS NULL OR v_product_name IS NULL OR v_category IS NULL OR v_price IS NULL OR v_stock_quantity IS NULL THEN
            RAISE ex_invalid_data;
        END IF;

        -- Check if the product already exists
        SELECT COUNT(*)
        INTO v_product_exists
        FROM products
        WHERE product_id = v_product_id;

        IF v_product_exists = 0 THEN
            RAISE ex_invalid_data;
        END IF;
    END validate_data;

    PROCEDURE log_error(p_product_id IN NUMBER, p_message IN VARCHAR2) IS
    BEGIN
        INSERT INTO error_log (error_date, product_id, error_message)
        VALUES (SYSDATE, p_product_id, p_message);
        COMMIT;
    END log_error;

BEGIN
    OPEN products_cursor;
    LOOP
        FETCH products_cursor INTO v_product_id, v_product_name, v_category, v_price, v_stock_quantity;
        EXIT WHEN products_cursor%NOTFOUND;

        BEGIN
            -- Validate data
            validate_data;

            -- Insert or update product record
            MERGE INTO products p
            USING (SELECT v_product_id AS product_id,
                          v_product_name AS product_name,
                          v_category AS category,
                          v_price AS price,
                          v_stock_quantity AS stock_quantity
                   FROM dual) s
            ON (p.product_id = s.product_id)
            WHEN MATCHED THEN
                UPDATE SET p.product_name = s.product_name,
                           p.category = s.category,
                           p.price = s.price,
                           p.stock_quantity = s.stock_quantity
            WHEN NOT MATCHED THEN
                INSERT (product_id, product_name, category, price, stock_quantity)
                VALUES (s.product_id, s.product_name, s.category, s.price, s.stock_quantity);

        EXCEPTION
            WHEN ex_invalid_data THEN
                log_error(v_product_id, 'Invalid product data');
            WHEN OTHERS THEN
                log_error(v_product_id, SQLERRM);
        END;
    END LOOP;
    CLOSE products_cursor;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END integrate_products_data;
"""

# Convert Oracle procedure to Snowflake procedure
snowflake_procedure = convert_oracle_to_snowflake(oracle_procedure)
print(snowflake_procedure)

# Use Snowflake connector to execute the Snowflake procedure (Example, you need to set up your connection parameters)
conn = snowflake.connector.connect(
    user='ESWARMANIKANTA',
    password='Eswar@7185',
    account='hd90197.europe-west4.gcp',
    warehouse='COMPUTE_WH',
    database='PUBLIC',
    schema='PUBLIC'
)

cur = conn.cursor()

def execute_sql(command):
    try:
        cur.execute(command)
        print(f"Executed: {command}")
    except Exception as e:
        print(f"Error executing {command}: {e}")

# Execute the converted Snowflake procedure
execute_sql(snowflake_procedure)

# Execute the procedure call
execute_sql("CALL integrate_products_data();")

# Close the cursor and connection
cur.close()
conn.close()
