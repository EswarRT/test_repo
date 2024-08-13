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
    proc_name = proc_name_match.group(1)
    parameters = proc_name_match.group(2).strip()
    
    # Convert parameters
    param_list = []
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
    
    # Replace Oracle SQL functions with Snowflake SQL equivalents
    procedure_body = procedure_body.replace("SYSDATE", "CURRENT_TIMESTAMP")
    procedure_body = procedure_body.replace("COMMIT;", "")  # Snowflake auto-commits
    
    # Generate Snowflake stored procedure in JavaScript
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
CREATE OR REPLACE PROCEDURE update_supplier_contact (
    p_supplier_id IN NUMBER,
    p_new_contact_name IN VARCHAR2,
    p_new_contact_email IN VARCHAR2
) IS
BEGIN
    UPDATE SUPPLIERS
    SET CONTACT_NAME = p_new_contact_name,
        CONTACT_EMAIL = p_new_contact_email
    WHERE SUPPLIER_ID = p_supplier_id;

    COMMIT;
END;
/

BEGIN
    update_supplier_contact(103, 'Eswar', 'david.wright@audiopro.com');
END;
/
"""
    # return oracle_procedure

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
