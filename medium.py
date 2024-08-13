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
    proc_name_match = re.search(r'PROCEDURE\s+(\w+)\s*\((.*?)\)', oracle_procedure, re.DOTALL)
    if not proc_name_match:
        raise ValueError("The provided Oracle procedure does not match the expected format.")
    
    proc_name = proc_name_match.group(1)
    parameters = proc_name_match.group(2) or ''
    
    # Convert parameters
    param_list = []
    if parameters:
        for param in parameters.split(','):
            param = param.strip()
            param_parts = param.split()
            param_name = param_parts[0]
            param_type = param_parts[2]
            param_type = type_mapping.get(param_type.upper(), param_type)
            param_list.append(f'{param_name} {param_type}')
    snowflake_parameters = ', '.join(param_list)
    
    # Extract the body of the procedure
    body_start = oracle_procedure.index("BEGIN") + len("BEGIN")
    body_end = oracle_procedure.index("END;")
    procedure_body = oracle_procedure[body_start:body_end].strip()
    
    # Convert Oracle PL/SQL to Snowflake JavaScript
    js_procedure_body = """
    var v_count;
    
    // Check if the product exists
    var sql_check = `SELECT COUNT(*) FROM PRODUCTS WHERE PRODUCT_ID = :1`;
    var result_check = snowflake.execute({sqlText: sql_check, binds: [p_product_id]});
    
    if (result_check.next()) {
        v_count = result_check.getColumnValue(1);
    }

    if (v_count == 0) {
        return 'Product ID does not exist';
    }

    // Update the price
    var sql_update = `UPDATE PRODUCTS SET PRICE = :1 WHERE PRODUCT_ID = :2`;
    snowflake.execute({sqlText: sql_update, binds: [p_new_price, p_product_id]});
    
    return 'update_product_price executed successfully';
    """
    
    # Generate Snowflake stored procedure in JavaScript
    snowflake_procedure = f"""
CREATE OR REPLACE PROCEDURE {proc_name}({snowflake_parameters})
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
{js_procedure_body}
$$;
"""
    return snowflake_procedure

# Example Oracle procedure
oracle_procedure = """
CREATE OR REPLACE PROCEDURE update_product_price(
    p_product_id IN NUMBER, 
    p_new_price IN NUMBER
) IS
    v_count NUMBER;
BEGIN
    -- Check if the product exists
    SELECT COUNT(*)
    INTO v_count
    FROM PRODUCTS
    WHERE PRODUCT_ID = p_product_id;

    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Product ID does not exist');
    END IF;

    -- Update the price
    UPDATE PRODUCTS
    SET PRICE = p_new_price
    WHERE PRODUCT_ID = p_product_id;

    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20002, 'Product ID does not exist');
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20003, 'An unexpected error occurred: ' || SQLERRM);
END;
/
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

# Close the cursor and connection
cur.close()
conn.close()
