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
    proc_name_match = re.search(r'PROCEDURE\s+(\w+)\s*\((.*?)\)\s*IS', oracle_procedure, re.DOTALL)
    if not proc_name_match:
        raise ValueError("The provided Oracle procedure does not match the expected format.")
    
    proc_name = proc_name_match.group(1)
    parameters = proc_name_match.group(2).strip()
    
    # Convert parameters
    param_list = []
    param_declarations = []
    param_assignments = []
    param_defaults = []
    if parameters:
        for param in re.split(r',\s*(?=[^)]*(?:\(|$))', parameters):
            param = param.strip()
            param_parts = re.split(r'\s+', param)
            if len(param_parts) < 3:
                raise ValueError(f"Parameter definition '{param}' does not match the expected format.")
            param_name = param_parts[0]
            param_type = param_parts[2]
            param_direction = param_parts[1].upper()
            param_type = type_mapping.get(param_type.upper(), param_type)
            param_list.append(f'{param_name} {param_type}')
            param_declarations.append(f'var {param_name} = arguments.{param_name};')
            if param_direction == 'OUT':
                param_assignments.append(f'arguments.{param_name} = "";')
            if 'DEFAULT' in param.upper():
                param_default = re.search(r'DEFAULT\s+(.*)', param, re.IGNORECASE).group(1)
                param_defaults.append(f'if ({param_name} === undefined) {{ {param_name} = {param_default}; }}')
    snowflake_parameters = ', '.join(param_list)
    
    # Extract the body of the procedure
    body_match = re.search(r'BEGIN\s+(.*)\s+EXCEPTION', oracle_procedure, re.DOTALL)
    if not body_match:
        print(f"Error: Could not extract body for procedure '{proc_name}'")
        raise ValueError("The procedure body does not match the expected format.")
    
    procedure_body = body_match.group(1).strip()
    
    # Replace Oracle SQL functions with Snowflake SQL equivalents
    procedure_body = procedure_body.replace("SYSDATE", "CURRENT_TIMESTAMP")
    procedure_body = procedure_body.replace("COMMIT;", "")  # Snowflake auto-commits

    # Replace PL/SQL logic with JavaScript logic
    procedure_body = re.sub(r'IF\s+(.+?)\s+THEN', r'if (\1) {', procedure_body)
    procedure_body = re.sub(r'ELSIF\s+(.+?)\s+THEN', r'} else if (\1) {', procedure_body)
    procedure_body = re.sub(r'ELSE\s+', r'} else {', procedure_body)
    procedure_body = re.sub(r'END\s+IF;', r'}', procedure_body)
    procedure_body = re.sub(r'LOOP\s+', r'while (true) {', procedure_body)
    procedure_body = re.sub(r'END\s+LOOP;', r'}', procedure_body)

    # Replace SQL statements with JavaScript SQL execution logic
    procedure_body = re.sub(r'SELECT (.+?) INTO (.+?) FROM (.+?);', r'''
var query = `SELECT \1 FROM \3`;
var statement = snowflake.createStatement({sqlText: query});
var result = statement.execute();
if (result.next()) {
    \2 = result.getColumnValue(1);
}
''', procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'INSERT INTO (.+?) \((.+?)\) VALUES \((.+?)\);', r'''
var query = `INSERT INTO \1 (\2) VALUES (\3)`;
executeSQL(query);
''', procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'UPDATE (.+?) SET (.+?) WHERE (.+?);', r'''
var query = `UPDATE \1 SET \2 WHERE \3`;
executeSQL(query);
''', procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'DELETE FROM (.+?) WHERE (.+?);', r'''
var query = `DELETE FROM \1 WHERE \2`;
executeSQL(query);
''', procedure_body, flags=re.DOTALL)

    # Replace RAISE_APPLICATION_ERROR with throw
    procedure_body = re.sub(r'RAISE_APPLICATION_ERROR\(-\d+, (.+?)\);', r'throw new Error(\1);', procedure_body)

    # Generate Snowflake stored procedure in JavaScript
    snowflake_procedure = f"""
CREATE OR REPLACE PROCEDURE {proc_name}(
    {snowflake_parameters}
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var result = '';
try {{
    var sql_command;
    var statement;

    // Helper function to execute SQL commands
    function executeSQL(sql_command) {{
        statement = snowflake.createStatement({{sqlText: sql_command}});
        return statement.execute();
    }}

    // Declare input parameters
    { " ".join(param_declarations) }

    // Handle default values
    { " ".join(param_defaults) }

    // Initialize output parameters
    { " ".join(param_assignments) }

    // Translated procedure body
    {procedure_body}

    result = 'Procedure executed successfully';
}} catch (err) {{
    result = 'An error occurred: ' + err.message;
}}
return result;
$$;
"""
    return snowflake_procedure

# Example Oracle procedure
oracle_procedure = """
CREATE OR REPLACE PROCEDURE manage_emp_data (
    p_action IN VARCHAR2,
    p_employee_id IN NUMBER,
    p_first_name IN VARCHAR2 DEFAULT NULL,
    p_last_name IN VARCHAR2 DEFAULT NULL,
    p_department_id IN NUMBER DEFAULT NULL,
    p_salary IN NUMBER DEFAULT NULL,
    p_hire_date IN DATE DEFAULT NULL,
    p_manager_id IN NUMBER DEFAULT NULL,
    p_status IN VARCHAR2 DEFAULT NULL,
    p_report_date IN DATE DEFAULT SYSDATE
) IS
    -- Define a cursor for reporting
    CURSOR emp_cursor IS
        SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, DEPARTMENT_ID, SALARY, HIRE_DATE, MANAGER_ID
        FROM EMP
        WHERE STATUS = 'ACTIVE' AND HIRE_DATE > p_report_date;

    -- Variable for row from cursor
    emp_record EMP%ROWTYPE;
    
    -- Dynamic SQL statement
    v_sql VARCHAR2(1000);
    
    -- Error message variable
    v_error_message VARCHAR2(4000);
    
    -- Counter for processed rows
    v_row_count NUMBER;
    
BEGIN
    IF p_action = 'INSERT' THEN
        -- Check if employee already exists
        SELECT COUNT(*) INTO v_row_count
        FROM EMP
        WHERE EMPLOYEE_ID = p_employee_id;
        
        IF v_row_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Employee ID already exists');
        END IF;
        
        -- Insert new employee
        INSERT INTO EMP (
            EMPLOYEE_ID, FIRST_NAME, LAST_NAME, DEPARTMENT_ID, SALARY, HIRE_DATE, MANAGER_ID, STATUS, LAST_UPDATED
        ) VALUES (
            p_employee_id, p_first_name, p_last_name, p_department_id, p_salary, p_hire_date, p_manager_id, 'ACTIVE', SYSDATE
        );
        
        DBMS_OUTPUT.PUT_LINE('Employee inserted successfully');

    ELSIF p_action = 'UPDATE' THEN
        -- Check if employee exists
        SELECT COUNT(*) INTO v_row_count
        FROM EMP
        WHERE EMPLOYEE_ID = p_employee_id;
        
        IF v_row_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20002, 'Employee ID does not exist');
        END IF;
        
        -- Update employee details
        UPDATE EMP
        SET FIRST_NAME = NVL(p_first_name, FIRST_NAME),
            LAST_NAME = NVL(p_last_name, LAST_NAME),
            DEPARTMENT_ID = NVL(p_department_id, DEPARTMENT_ID),
            SALARY = NVL(p_salary, SALARY),
            HIRE_DATE = NVL(p_hire_date, HIRE_DATE),
            MANAGER_ID = NVL(p_manager_id, MANAGER_ID),
            STATUS = NVL(p_status, STATUS),
            LAST_UPDATED = SYSDATE
        WHERE EMPLOYEE_ID = p_employee_id;
        
        DBMS_OUTPUT.PUT_LINE('Employee updated successfully');

    ELSIF p_action = 'DELETE' THEN
        -- Check if employee exists
        SELECT COUNT(*) INTO v_row_count
        FROM EMP
        WHERE EMPLOYEE_ID = p_employee_id;
        
        IF v_row_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20003, 'Employee ID does not exist');
        END IF;
        
        -- Delete employee
        DELETE FROM EMP
        WHERE EMPLOYEE_ID = p_employee_id;
        
        DBMS_OUTPUT.PUT_LINE('Employee deleted successfully');

    ELSIF p_action = 'SELECT' THEN
        -- Fetch and display employee details
        FOR emp_record IN (
            SELECT * FROM EMP
            WHERE EMPLOYEE_ID = p_employee_id
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('Employee ID: ' || emp_record.EMPLOYEE_ID);
            DBMS_OUTPUT.PUT_LINE('First Name: ' || emp_record.FIRST_NAME);
            DBMS_OUTPUT.PUT_LINE('Last Name: ' || emp_record.LAST_NAME);
            DBMS_OUTPUT.PUT_LINE('Department ID: ' || emp_record.DEPARTMENT_ID);
            DBMS_OUTPUT.PUT_LINE('Salary: ' || emp_record.SALARY);
            DBMS_OUTPUT.PUT_LINE('Hire Date: ' || emp_record.HIRE_DATE);
            DBMS_OUTPUT.PUT_LINE('Manager ID: ' || emp_record.MANAGER_ID);
        END LOOP;

    ELSIF p_action = 'REPORT' THEN
        -- Generate a report of EMP hired after a specific date
        FOR emp_record IN emp_cursor LOOP
            DBMS_OUTPUT.PUT_LINE('Employee ID: ' || emp_record.EMPLOYEE_ID);
            DBMS_OUTPUT.PUT_LINE('First Name: ' || emp_record.FIRST_NAME);
            DBMS_OUTPUT.PUT_LINE('Last Name: ' || emp_record.LAST_NAME);
            DBMS_OUTPUT.PUT_LINE('Department ID: ' || emp_record.DEPARTMENT_ID);
            DBMS_OUTPUT.PUT_LINE('Salary: ' || emp_record.SALARY);
            DBMS_OUTPUT.PUT_LINE('Hire Date: ' || emp_record.HIRE_DATE);
            DBMS_OUTPUT.PUT_LINE('Manager ID: ' || emp_record.MANAGER_ID);
            DBMS_OUTPUT.PUT_LINE('-----------------------------');
        END LOOP;

    ELSIF p_action = 'DYNAMIC_SQL' THEN
        -- Execute a dynamic SQL statement based on input
        v_sql := 'UPDATE EMP SET SALARY = SALARY * 1.1 WHERE DEPARTMENT_ID = :dept_id';
        
        EXECUTE IMMEDIATE v_sql USING p_department_id;
        
        DBMS_OUTPUT.PUT_LINE('Salaries updated dynamically for department ID ' || p_department_id);

    ELSE
        v_error_message := 'Invalid action: ' || p_action;
        RAISE_APPLICATION_ERROR(-20004, v_error_message);
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        v_error_message := SQLERRM;
        DBMS_OUTPUT.PUT_LINE('Error: ' || v_error_message);
        RAISE;
END manage_emp_data;
/
BEGIN
    manage_emp_data(
        p_action        => 'INSERT',
        p_employee_id   => 11,
        p_first_name    => 'Tom',
        p_last_name     => 'Hanks',
        p_department_id => 106,
        p_salary        => 60000.00,
        p_hire_date     => TO_DATE('2024-01-10', 'YYYY-MM-DD'),
        p_manager_id    => 1
    );
END;
/

BEGIN
    manage_employee_data(
        p_action        => 'UPDATE',
        p_employee_id   => 10,
        p_first_name    => 'Hannah',
        p_last_name     => 'Roberts',
        p_salary        => 62000.00
    );
END;
/

BEGIN
    manage_employee_data(
        p_action        => 'DELETE',
        p_employee_id   => 11
    );
END;
/

BEGIN
    manage_employee_data(
        p_action        => 'SELECT',
        p_employee_id   => 1
    );
END;
/
"""

# Convert Oracle procedure to Snowflake procedure
try:
    snowflake_procedure = convert_oracle_to_snowflake(oracle_procedure)
    print(snowflake_procedure)
except ValueError as e:
    print(e)

# Use Snowflake connector to execute the Snowflake procedure
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
if 'snowflake_procedure' in locals():
    execute_sql(snowflake_procedure)
else:
    print("Snowflake procedure was not generated successfully.")

# Execute the procedure call with appropriate parameters
def call_procedure(proc_name, *args):
    placeholders = ', '.join(['%s'] * len(args))
    sql_command = f"CALL {proc_name}({placeholders})"
    try:
        cur.execute(sql_command, args)
        result = cur.fetchone()
        if result:
            print(f"Procedure executed successfully: {result[0]}")
        else:
            print("Procedure executed successfully with no return value.")
    except Exception as e:
        print(f"Error executing procedure: {e}")

# Example call to the Snowflake stored procedure
# call_procedure("manage_emp_data", 'INSERT', 101, 'John', 'Doe', 10, 60000, '2021-01-01', 5, 'ACTIVE', '2021-12-31')

# Close the cursor and connection
cur.close()
conn.close()
