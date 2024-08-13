import re
import snowflake.connector

def convert_oracle_to_snowflake(oracle_procedure):
    # Extract the procedure name and parameters
    proc_name_match = re.search(r'CREATE OR REPLACE PROCEDURE\s+(\w+)\s+\((.*?)\)\s+IS', oracle_procedure, re.DOTALL)
    if not proc_name_match:
        raise ValueError("The provided Oracle procedure does not match the expected format.")

    proc_name = proc_name_match.group(1)
    parameters = proc_name_match.group(2).strip()

    # Convert parameters
    param_list = []
    param_declarations = []
    if parameters:
        for param in re.split(r',\s*(?=[^)]*(?:\(|$))', parameters):
            param = param.strip()
            param_parts = re.split(r'\s+', param)
            if len(param_parts) < 3:
                raise ValueError(f"Parameter definition '{param}' does not match the expected format.")
            param_name = param_parts[0]
            param_list.append(f'{param_name} STRING')  # Using STRING for simplicity
            param_declarations.append(f'var {param_name} = arguments[{len(param_declarations)}];')

    snowflake_parameters = ', '.join(param_list)

    # Extract the body of the procedure
    body_match = re.search(r'BEGIN\s+(.*)\s+EXCEPTION', oracle_procedure, re.DOTALL)
    if not body_match:
        print(f"Error: Could not extract body for procedure '{proc_name}'")
        raise ValueError("The procedure body does not match the expected format.")

    procedure_body = body_match.group(1).strip()

    # Replace Oracle SQL functions with Snowflake SQL equivalents
    procedure_body = procedure_body.replace("SYSDATE", "CURRENT_DATE")
    procedure_body = procedure_body.replace("DBMS_OUTPUT.PUT_LINE", "console.log")
    procedure_body = procedure_body.replace("COMMIT;", "")  # Snowflake auto-commits

    # Replace PL/SQL logic with JavaScript logic
    procedure_body = re.sub(r'IF\s+(.+?)\s+THEN', r'if (\1) {', procedure_body)
    procedure_body = re.sub(r'ELSIF\s+(.+?)\s+THEN', r'} else if (\1) {', procedure_body)
    procedure_body = re.sub(r'ELSE\s+', r'} else {', procedure_body)
    procedure_body = re.sub(r'END\s+IF;', r'}', procedure_body)
    procedure_body = re.sub(r'LOOP\s+', r'while (true) {', procedure_body)
    procedure_body = re.sub(r'END\s+LOOP;', r'}', procedure_body)

    # Replace SQL statements with JavaScript SQL execution logic
    procedure_body = re.sub(r'SELECT\s+(.*?)\s+INTO\s+(.*?)\s+FROM\s+(.*?);', r"""
var query = `SELECT \1 FROM \3`;
var statement = snowflake.createStatement({sqlText: query});
var result = statement.execute();
if (result.next()) {
    \2 = result.getColumnValue(1);
}
""", procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'INSERT INTO\s+(.*?)\s+\((.*?)\)\s+VALUES\s+\((.*?)\);', r"""
var query = `INSERT INTO \1 (\2) VALUES (\3)`;
snowflake.execute({sqlText: query});
""", procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'UPDATE\s+(.*?)\s+SET\s+(.*?)\s+WHERE\s+(.*?);', r"""
var query = `UPDATE \1 SET \2 WHERE \3`;
snowflake.execute({sqlText: query});
""", procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'DELETE FROM\s+(.*?)\s+WHERE\s+(.*?);', r"""
var query = `DELETE FROM \1 WHERE \2`;
snowflake.execute({sqlText: query});
""", procedure_body, flags=re.DOTALL)

    # Replace RAISE_APPLICATION_ERROR with throw
    procedure_body = re.sub(r'RAISE_APPLICATION_ERROR\(-\d+, (.+?)\);', r'throw new Error(\1);', procedure_body)

    # Replace PL/SQL exception handling with JavaScript try-catch
    procedure_body = re.sub(r'EXCEPTION\n\s+WHEN OTHERS THEN', r'} catch (err) {', procedure_body)
    procedure_body = re.sub(r'SQLERRM', r'err.message', procedure_body)
    procedure_body = re.sub(r'RAISE;', r'throw err;', procedure_body)
    procedure_body = re.sub(r'PRAGMA EXCEPTION_INIT\(.+?\);', '', procedure_body)

    # Correct comment syntax from PL/SQL to JavaScript
    procedure_body = re.sub(r'--', r'//', procedure_body)

    # Generate Snowflake stored procedure in JavaScript
    snowflake_procedure = f"""
CREATE OR REPLACE PROCEDURE {proc_name}({snowflake_parameters})
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var result = '';
try {{
    // Declare parameters
    {''.join(param_declarations)}

    // Helper function to execute SQL commands
    function executeSQL(sql_command) {{
        var statement = snowflake.createStatement({{sqlText: sql_command}});
        return statement.execute();
    }}

    // Action-based processing
    switch (p_action.toUpperCase()) {{
        case 'INSERT':
            if (!p_first_name || !p_last_name || !p_department_id || !p_salary || !p_hire_date) {{
                throw new Error('All parameters must be provided for INSERT.');
            }}
            var emp_cursor = `SELECT employee_id, first_name, last_name, department_id, salary, hire_date
                              FROM employee WHERE employee_id = ` + p_employee_id;
            var emp_rs = executeSQL(emp_cursor);
            if (emp_rs.next()) {{
                throw new Error('Employee with this ID already exists.');
            }}
            var insert_sql = `INSERT INTO employee (employee_id, first_name, last_name, department_id, salary, hire_date)
                              VALUES (` + p_employee_id + `, '` + p_first_name + `', '` + p_last_name + `', ` + p_department_id + `, ` + p_salary + `, '` + p_hire_date + `')`;
            executeSQL(insert_sql);
            break;
        case 'UPDATE':
            if (!p_employee_id) {{
                throw new Error('Employee ID must be provided for UPDATE.');
            }}
            var emp_cursor = `SELECT employee_id, first_name, last_name, department_id, salary, hire_date
                              FROM employee WHERE employee_id = ` + p_employee_id;
            var emp_rs = executeSQL(emp_cursor);
            if (!emp_rs.next()) {{
                throw new Error('Employee with this ID does not exist.');
            }}
            var update_sql = `UPDATE employee SET first_name = '` + p_first_name + `', last_name = '` + p_last_name + `',
                              department_id = ` + p_department_id + `, salary = ` + p_salary + `, hire_date = '` + p_hire_date + `'
                              WHERE employee_id = ` + p_employee_id;
            executeSQL(update_sql);
            break;
        case 'DELETE':
            if (!p_employee_id) {{
                throw new Error('Employee ID must be provided for DELETE.');
            }}
            var emp_cursor = `SELECT employee_id, first_name, last_name, department_id, salary, hire_date
                              FROM employee WHERE employee_id = ` + p_employee_id;
            var emp_rs = executeSQL(emp_cursor);
            if (!emp_rs.next()) {{
                throw new Error('Employee with this ID does not exist.');
            }}
            var delete_sql = `DELETE FROM employee WHERE employee_id = ` + p_employee_id;
            executeSQL(delete_sql);
            break;
        case 'RETRIEVE':
            var emp_cursor = `SELECT employee_id, first_name, last_name, department_id, salary, hire_date
                              FROM employee WHERE employee_id = ` + p_employee_id;
            var emp_rs = executeSQL(emp_cursor);
            if (emp_rs.next()) {{
                console.log('Employee ID: ' + emp_rs.getColumnValue('EMPLOYEE_ID'));
                console.log('First Name: ' + emp_rs.getColumnValue('FIRST_NAME'));
                console.log('Last Name: ' + emp_rs.getColumnValue('LAST_NAME'));
                console.log('Department ID: ' + emp_rs.getColumnValue('DEPARTMENT_ID'));
                console.log('Salary: ' + emp_rs.getColumnValue('SALARY'));
                console.log('Hire Date: ' + emp_rs.getColumnValue('HIRE_DATE'));
            }} else {{
                console.log('No employee found with ID ' + p_employee_id);
            }}
            break;
        default:
            throw new Error('Invalid action specified.');
    }}

    result = 'Procedure executed successfully.';
}} catch (err) {{
    result = 'An error occurred: ' + err.message;
}}
return result;
$$;
"""
    return snowflake_procedure

# Example Oracle procedure
oracle_procedure = """
CREATE OR REPLACE PROCEDURE manage_employee (
    p_action         IN VARCHAR2,
    p_employee_id    IN NUMBER DEFAULT NULL,
    p_first_name     IN VARCHAR2 DEFAULT NULL,
    p_last_name      IN VARCHAR2 DEFAULT NULL,
    p_department_id  IN NUMBER DEFAULT NULL,
    p_salary         IN NUMBER DEFAULT NULL,
    p_hire_date      IN DATE DEFAULT NULL
)
IS
    -- Declare a local variable to hold the cursor result
    CURSOR emp_cursor IS
        SELECT employee_id, first_name, last_name, department_id, salary, hire_date
        FROM employee
        WHERE employee_id = p_employee_id;

    v_emp_record emp_cursor%ROWTYPE;
    
    -- Declare a variable to hold the dynamic SQL
    v_sql VARCHAR2(4000);
    
    -- Exception handling variables
    e_no_data_found EXCEPTION;
    e_invalid_action EXCEPTION;
    e_invalid_salary EXCEPTION;
    
BEGIN
    -- Start transaction
    SAVEPOINT sp_manage_employee;
    
    -- Action-based processing
    CASE p_action
        WHEN 'INSERT' THEN
            -- Validate inputs
            IF p_first_name IS NULL OR p_last_name IS NULL OR p_department_id IS NULL OR p_salary IS NULL OR p_hire_date IS NULL THEN
                RAISE_APPLICATION_ERROR(-20001, 'All parameters must be provided for INSERT.');
            END IF;
            
            -- Check if employee_id already exists
            OPEN emp_cursor;
            FETCH emp_cursor INTO v_emp_record;
            IF emp_cursor%FOUND THEN
                RAISE_APPLICATION_ERROR(-20002, 'Employee with this ID already exists.');
            END IF;
            CLOSE emp_cursor;
            
            -- Insert new employee
            INSERT INTO employee (employee_id, first_name, last_name, department_id, salary, hire_date)
            VALUES (p_employee_id, p_first_name, p_last_name, p_department_id, p_salary, p_hire_date);
        
        WHEN 'UPDATE' THEN
            -- Validate inputs
            IF p_employee_id IS NULL THEN
                RAISE_APPLICATION_ERROR(-20003, 'Employee ID must be provided for UPDATE.');
            END IF;
            
            -- Check if employee exists
            OPEN emp_cursor;
            FETCH emp_cursor INTO v_emp_record;
            IF NOT emp_cursor%FOUND THEN
                RAISE_APPLICATION_ERROR(-20004, 'Employee with this ID does not exist.');
            END IF;
            CLOSE emp_cursor;
            
            -- Update employee record
            v_sql := 'UPDATE employee SET first_name = :1, last_name = :2, department_id = :3, salary = :4, hire_date = :5 WHERE employee_id = :6';
            EXECUTE IMMEDIATE v_sql USING p_first_name, p_last_name, p_department_id, p_salary, p_hire_date, p_employee_id;
        
        WHEN 'DELETE' THEN
            -- Validate inputs
            IF p_employee_id IS NULL THEN
                RAISE_APPLICATION_ERROR(-20005, 'Employee ID must be provided for DELETE.');
            END IF;
            
            -- Check if employee exists
            OPEN emp_cursor;
            FETCH emp_cursor INTO v_emp_record;
            IF NOT emp_cursor%FOUND THEN
                RAISE_APPLICATION_ERROR(-20006, 'Employee with this ID does not exist.');
            END IF;
            CLOSE emp_cursor;
            
            -- Delete employee record
            DELETE FROM employee WHERE employee_id = p_employee_id;
        
        WHEN 'RETRIEVE' THEN
            -- Retrieve employee details
            OPEN emp_cursor;
            FETCH emp_cursor INTO v_emp_record;
            IF emp_cursor%FOUND THEN
                DBMS_OUTPUT.PUT_LINE('Employee ID: ' || v_emp_record.employee_id);
                DBMS_OUTPUT.PUT_LINE('First Name: ' || v_emp_record.first_name);
                DBMS_OUTPUT.PUT_LINE('Last Name: ' || v_emp_record.last_name);
                DBMS_OUTPUT.PUT_LINE('Department ID: ' || v_emp_record.department_id);
                DBMS_OUTPUT.PUT_LINE('Salary: ' || v_emp_record.salary);
                DBMS_OUTPUT.PUT_LINE('Hire Date: ' || TO_CHAR(v_emp_record.hire_date, 'YYYY-MM-DD'));
            ELSE
                DBMS_OUTPUT.PUT_LINE('No employee found with ID ' || p_employee_id);
            END IF;
            CLOSE emp_cursor;
        
        ELSE
            RAISE e_invalid_action;
    END CASE;
    
    -- Commit the transaction
    COMMIT;
    
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Handle the case when no data is found
        ROLLBACK TO sp_manage_employee;
        DBMS_OUTPUT.PUT_LINE('No data found.');
    
    WHEN e_invalid_action THEN
        -- Handle invalid action error
        ROLLBACK TO sp_manage_employee;
        DBMS_OUTPUT.PUT_LINE('Invalid action specified.');
    
    WHEN e_invalid_salary THEN
        -- Handle invalid salary error
        ROLLBACK TO sp_manage_employee;
        DBMS_OUTPUT.PUT_LINE('Invalid salary amount provided.');
    
    WHEN OTHERS THEN
        -- Handle other exceptions
        ROLLBACK TO sp_manage_employee;
        DBMS_OUTPUT.PUT_LINE('An unexpected error occurred: ' || SQLERRM);
    
END manage_employee;
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
def call_procedure(proc_name, *params):
    placeholders = ', '.join(['%s'] * len(params))
    sql_command = f"CALL {proc_name}({placeholders})"
    try:
        cur.execute(sql_command, params)
        result = cur.fetchone()
        if result:
            print(f"Procedure executed successfully: {result[0]}")
        else:
            print("Procedure executed successfully with no return value.")
    except Exception as e:
        print(f"Error executing procedure: {e}")

# Example call to the Snowflake stored procedure
call_procedure("manage_employee", 'INSERT', '107', 'John', 'Doe', '10', '60000', '2021-01-01')
call_procedure("manage_employee", 'UPDATE', '107', 'John', 'Doe', '10', '65000', '2021-01-01')
call_procedure("manage_employee", 'DELETE', '107', 'John', 'Doe', '10', '65000', '2021-01-01')


# Close the cursor and connection
cur.close()
conn.close()
#working
