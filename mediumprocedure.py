import re
import snowflake.connector

# Function to convert Oracle PL/SQL to Snowflake SQL stored procedure
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
    parameters = proc_name_match.group(2).strip()
    
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
    body_match = re.search(r'BEGIN\s+(.*?)\s+END\s*;', oracle_procedure, re.DOTALL)
    if not body_match:
        print(f"Error: The procedure body could not be extracted from: {oracle_procedure}")
        raise ValueError("The procedure body could not be extracted.")

    procedure_body = body_match.group(1).strip()

    # Debug: print the extracted procedure body
    print(f"Extracted procedure body: {procedure_body}")

    # Replace Oracle SQL functions with Snowflake SQL equivalents
    procedure_body = procedure_body.replace("SYSDATE", "CURRENT_TIMESTAMP")
    procedure_body = procedure_body.replace("COMMIT;", "")  # Snowflake auto-commits

    # Generate Snowflake stored procedure in SQL
    snowflake_procedure = f"""
CREATE OR REPLACE PROCEDURE {proc_name}({snowflake_parameters})
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    {procedure_body}

    RETURN '{proc_name} executed successfully';
END;
$$;
"""
    return snowflake_procedure

# Example Oracle procedure
oracle_procedure = """
CREATE OR REPLACE PROCEDURE process_product_updates (
    category IN VARCHAR2
) IS
    CURSOR product_cursor IS
        SELECT product_id, price
        FROM products
        WHERE category = category;
    
    product_id products.product_id%TYPE;
    price products.price%TYPE;
BEGIN
    FOR product_record IN product_cursor LOOP
        product_id := product_record.product_id;
        price := product_record.price;
        
        -- Example update based on cursor data
        UPDATE products
        SET PRICE = price * 1.1
        WHERE PRODUCT_ID = product_id;
    END LOOP;
END process_product_updates;
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
