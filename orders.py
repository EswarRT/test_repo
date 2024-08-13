# import re
# import snowflake.connector

# # Function to convert Oracle PL/SQL to Snowflake JavaScript stored procedure
# def convert_oracle_to_snowflake(oracle_procedure):
#     # Define a simple mapping of Oracle to Snowflake data types and constructs
#     type_mapping = {
#         'NUMBER': 'FLOAT',
#         'VARCHAR2': 'STRING',
#         'DATE': 'DATE'
#     }
    
#     # Extract the procedure name and parameters
#     proc_name_match = re.search(r'PROCEDURE\s+(\w+)\s*\((.*?)\)', oracle_procedure, re.DOTALL)
#     proc_name = proc_name_match.group(1)
#     parameters = proc_name_match.group(2).strip()
    
#     # Convert parameters
#     param_list = []
#     for param in parameters.split(','):
#         param = param.strip()
#         param_parts = param.split()
#         param_name = param_parts[0]
#         param_type = param_parts[2]
#         param_type = type_mapping.get(param_type.upper(), param_type)
#         param_list.append(f'{param_name} {param_type}')
#     snowflake_parameters = ', '.join(param_list)
    
#     # Extract the body of the procedure
#     body_start = oracle_procedure.index("BEGIN") + len("BEGIN")
#     body_end = oracle_procedure.index("END;")
#     procedure_body = oracle_procedure[body_start:body_end].strip()
    
#     # Replace Oracle SQL functions with Snowflake SQL equivalents
#     procedure_body = procedure_body.replace("SYSDATE", "CURRENT_TIMESTAMP")
#     procedure_body = procedure_body.replace("COMMIT;", "")  # Snowflake auto-commits
    
#     # Generate Snowflake stored procedure in SQL
#     snowflake_procedure = f"""
# CREATE OR REPLACE PROCEDURE {proc_name}({snowflake_parameters})
# RETURNS STRING
# LANGUAGE SQL
# AS
# $$
# BEGIN
#     {procedure_body}

#     RETURN '{proc_name} executed successfully';
# END;
# $$;
# """
#     return snowflake_procedure

# # Example Oracle procedure
# oracle_procedure = """
# CREATE OR REPLACE PROCEDURE update_employee_salary(p_employee_id IN NUMBER, p_new_salary IN NUMBER) IS
#     v_old_salary EMPLOYEES.SALARY%TYPE;
#     v_status VARCHAR2(20);
#     v_error_message VARCHAR2(4000);
# BEGIN
#     -- Validate new salary
#     IF p_new_salary IS NULL THEN
#         RAISE_APPLICATION_ERROR(-20001, 'New salary cannot be null');
#     ELSIF p_new_salary <= 0 THEN
#         RAISE_APPLICATION_ERROR(-20002, 'New salary must be greater than zero');
#     END IF;

#     -- Fetch the old salary
#     SELECT SALARY INTO v_old_salary
#     FROM EMPLOYEES
#     WHERE EMPLOYEE_ID = p_employee_id;

#     -- Update the salary
#     UPDATE EMPLOYEES
#     SET SALARY = p_new_salary,
#         LAST_UPDATE_TIMESTAMP = SYSDATE,
#         LAST_UPDATE_STATUS = 'SUCCESS',
#         LAST_UPDATE_ERROR_MESSAGE = NULL
#     WHERE EMPLOYEE_ID = p_employee_id;

#     COMMIT;

# EXCEPTION
#     WHEN OTHERS THEN
#         -- Handle exceptions
#         v_status := 'FAILED';
#         v_error_message := SQLERRM;

#         -- Log the error in the EMPLOYEES table
#         UPDATE EMPLOYEES
#         SET LAST_UPDATE_TIMESTAMP = SYSDATE,
#             LAST_UPDATE_STATUS = v_status,
#             LAST_UPDATE_ERROR_MESSAGE = v_error_message
#         WHERE EMPLOYEE_ID = p_employee_id;

#         ROLLBACK;
#         RAISE;
# END;
# /

# BEGIN
#     update_employee_salary(4, 70000);
# END;
# / 
# """
#     # return oracle_procedure

# # Convert Oracle procedure to Snowflake procedure
# snowflake_procedure = convert_oracle_to_snowflake(oracle_procedure)
# print(snowflake_procedure)

# # Use Snowflake connector to execute the Snowflake procedure (Example, you need to set up your connection parameters)
# conn = snowflake.connector.connect(
#     user='ESWARMANIKANTA',
#     password='Eswar@7185',
#     account='hd90197.europe-west4.gcp',
#     warehouse='COMPUTE_WH',
#     database='PUBLIC',
#     schema='PUBLIC'
# )

# cur = conn.cursor()

# def execute_sql(command):
#     try:
#         cur.execute(command)
#         print(f"Executed: {command}")
#     except Exception as e:
#         print(f"Error executing {command}: {e}")

# # Execute the converted Snowflake procedure
# execute_sql(snowflake_procedure)

# # Close the cursor and connection
# cur.close()
# conn.close()
# ---------------------------------------------------------------------------
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
    
    # Convert the body to Snowflake JavaScript syntax
    js_procedure_body = """
    try {
        // Validate new salary
        if (P_NEW_SALARY === null) {
            throw new Error('New salary cannot be null');
        } else if (P_NEW_SALARY <= 0) {
            throw new Error('New salary must be greater than zero');
        }
        
        // Fetch the old salary
        var result = snowflake.execute(`SELECT SALARY INTO :v_old_salary FROM EMPLOYEES WHERE EMPLOYEE_ID = :P_EMPLOYEE_ID`);
        var v_old_salary = result.next() ? result.getColumnValue('SALARY') : null;
        
        // Update the salary
        snowflake.execute(`UPDATE EMPLOYEES SET SALARY = :P_NEW_SALARY, LAST_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP, LAST_UPDATE_STATUS = 'SUCCESS', LAST_UPDATE_ERROR_MESSAGE = NULL WHERE EMPLOYEE_ID = :P_EMPLOYEE_ID`);
        
    } catch (err) {
        // Handle exceptions
        var v_status = 'FAILED';
        var v_error_message = err.message;
        
        // Log the error in the EMPLOYEES table
        snowflake.execute(`UPDATE EMPLOYEES SET LAST_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP, LAST_UPDATE_STATUS = :v_status, LAST_UPDATE_ERROR_MESSAGE = :v_error_message WHERE EMPLOYEE_ID = :P_EMPLOYEE_ID`);
        
        // Rethrow the error
        throw err;
    }
    """

    # Generate Snowflake stored procedure in JavaScript
    snowflake_procedure = f"""
CREATE OR REPLACE PROCEDURE {proc_name}({snowflake_parameters})
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
{js_procedure_body}
$$;
"""
    return snowflake_procedure

# Example Oracle procedure
oracle_procedure = """
CREATE OR REPLACE PROCEDURE update_employee_salary(p_employee_id IN NUMBER, p_new_salary IN NUMBER) IS
    v_old_salary EMPLOYEES.SALARY%TYPE;
    v_status VARCHAR2(20);
    v_error_message VARCHAR2(4000);
BEGIN
    -- Validate new salary
    IF p_new_salary IS NULL THEN
        RAISE_APPLICATION_ERROR(-20001, 'New salary cannot be null');
    ELSIF p_new_salary <= 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'New salary must be greater than zero');
    END IF;

    -- Fetch the old salary
    SELECT SALARY INTO v_old_salary
    FROM EMPLOYEES
    WHERE EMPLOYEE_ID = p_employee_id;

    -- Update the salary
    UPDATE EMPLOYEES
    SET SALARY = p_new_salary,
        LAST_UPDATE_TIMESTAMP = SYSDATE,
        LAST_UPDATE_STATUS = 'SUCCESS',
        LAST_UPDATE_ERROR_MESSAGE = NULL
    WHERE EMPLOYEE_ID = p_employee_id;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- Handle exceptions
        v_status := 'FAILED';
        v_error_message := SQLERRM;

        -- Log the error in the EMPLOYEES table
        UPDATE EMPLOYEES
        SET LAST_UPDATE_TIMESTAMP = SYSDATE,
            LAST_UPDATE_STATUS = v_status,
            LAST_UPDATE_ERROR_MESSAGE = v_error_message
        WHERE EMPLOYEE_ID = p_employee_id;

        ROLLBACK;
        RAISE;
END;
/

BEGIN
    update_employee_salary(4, 70000);
    
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
