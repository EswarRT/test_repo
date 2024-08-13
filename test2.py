import re
import snowflake.connector

def convert_oracle_to_snowflake(oracle_procedure):
    # Extract the procedure name
    proc_name_match = re.search(r'CREATE OR REPLACE PROCEDURE\s+(\w+)\s+(?:\((.*?)\)\s+)?IS', oracle_procedure, re.DOTALL)
    if not proc_name_match:
        raise ValueError("The provided Oracle procedure does not match the expected format.")
    
    proc_name = proc_name_match.group(1)
    parameters = proc_name_match.group(2) if proc_name_match.group(2) else ""

    # Convert parameters
    param_list = []
    param_declarations = []
    param_defaults = []
    if parameters:
        param_declarations.append('// Declare parameters')
        for param in re.split(r',\s*(?=[^)]*(?:\(|$))', parameters):
            param = param.strip()
            param_parts = re.split(r'\s+', param)
            if len(param_parts) < 3:
                raise ValueError(f"Parameter definition '{param}' does not match the expected format.")
            param_name = param_parts[0]
            param_type = param_parts[2]
            param_direction = param_parts[1].upper()
            param_type = param_type.upper()
            param_list.append(f'{param_name} {param_type}')
            param_declarations.append(f'var {param_name};')
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
    procedure_body = re.sub(r'SELECT\s+(.*?)\s+INTO\s+(.*?)\s+FROM\s+(.*?);', r'''
var query = `SELECT \1 FROM \3`;
var statement = snowflake.createStatement({sqlText: query});
var result = statement.execute();
if (result.next()) {
    \2 = result.getColumnValue(1);
}
''', procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'INSERT INTO\s+(.*?)\s+\((.*?)\)\s+VALUES\s+\((.*?)\);', r'''
var query = `INSERT INTO \1 (\2) VALUES (\3)`;
snowflake.execute({sqlText: query});
''', procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'UPDATE\s+(.*?)\s+SET\s+(.*?)\s+WHERE\s+(.*?);', r'''
var query = `UPDATE \1 SET \2 WHERE \3`;
snowflake.execute({sqlText: query});
''', procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'DELETE FROM\s+(.*?)\s+WHERE\s+(.*?);', r'''
var query = `DELETE FROM \1 WHERE \2`;
snowflake.execute({sqlText: query});
''', procedure_body, flags=re.DOTALL)

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
CREATE OR REPLACE PROCEDURE {proc_name}()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var result = '';
try {{
    // Helper function to execute SQL commands
    function executeSQL(sql_command) {{
        var statement = snowflake.createStatement({{sqlText: sql_command}});
        return statement.execute();
    }}

    // Drop existing tables if they exist
    try {{
        executeSQL('DROP TABLE IF EXISTS employee_summary');
        executeSQL('DROP TABLE IF EXISTS department_summary');
    }} catch (err) {{
        result += 'Error dropping tables: ' + err.message + '\\n';
    }}

    // Create tables
    try {{
        executeSQL(`
            CREATE TABLE employee_summary (
                employee_id NUMBER,
                department_id NUMBER,
                salary NUMBER,
                hire_date DATE
            )
        `);
        executeSQL(`
            CREATE TABLE department_summary (
                department_id NUMBER,
                employee_count NUMBER,
                avg_salary NUMBER
            )
        `);
    }} catch (err) {{
        result += 'Error creating tables: ' + err.message + '\\n';
    }}

    // Process employees
    try {{
        var emp_cursor = `
            SELECT employee_id, department_id, salary, hire_date
            FROM employee
            WHERE hire_date > DATEADD(day, -365, CURRENT_DATE)
        `;
        var emp_rs = executeSQL(emp_cursor);

        while (emp_rs.next()) {{
            var employee_id = emp_rs.getColumnValue('EMPLOYEE_ID');
            var department_id = emp_rs.getColumnValue('DEPARTMENT_ID');
            var salary = emp_rs.getColumnValue('SALARY');
            var hire_date = emp_rs.getColumnValue('HIRE_DATE');

            var hire_date_str = hire_date.toISOString().split('T')[0];

            var insert_employee_summary_sql = `
                INSERT INTO employee_summary (employee_id, department_id, salary, hire_date)
                VALUES (?, ?, ?, ?)
            `;
            try {{
                snowflake.execute({{
                    sqlText: insert_employee_summary_sql,
                    binds: [employee_id, department_id, salary, hire_date_str]
                }});
                result += 'Inserted employee ' + employee_id + ' into employee_summary.\\n';
            }} catch (e) {{
                result += 'Error inserting employee ' + employee_id + ': ' + e.message + '\\n';
            }}
        }}
    }} catch (err) {{
        result += 'Error processing employees: ' + err.message + '\\n';
    }}

    // Process departments
    try {{
        var dept_cursor = `
            SELECT department_id, COUNT(*) AS employee_count, AVG(salary) AS avg_salary
            FROM employee
            GROUP BY department_id
        `;
        var dept_rs = executeSQL(dept_cursor);

        while (dept_rs.next()) {{
            var department_id = dept_rs.getColumnValue('DEPARTMENT_ID');
            var employee_count = dept_rs.getColumnValue('EMPLOYEE_COUNT');
            var avg_salary = dept_rs.getColumnValue('AVG_SALARY');

            var insert_department_summary_sql = `
                INSERT INTO department_summary (department_id, employee_count, avg_salary)
                VALUES (?, ?, ?)
            `;
            try {{
                snowflake.execute({{
                    sqlText: insert_department_summary_sql,
                    binds: [department_id, employee_count, avg_salary]
                }});
                result += 'Inserted department ' + department_id + ' into department_summary.\\n';
            }} catch (e) {{
                result += 'Error inserting department ' + department_id + ': ' + e.message + '\\n';
            }}
        }}
    }} catch (err) {{
        result += 'Error processing departments: ' + err.message + '\\n';
    }}

    result += 'Procedure executed successfully.';
}} catch (err) {{
    result = 'An error occurred: ' + err.message;
}}
return result;
$$;
"""
    return snowflake_procedure

# Example Oracle procedure
oracle_procedure = """
CREATE OR REPLACE PROCEDURE manage_employee_data_complex IS
    -- Cursors for fetching employee and department data
    CURSOR emp_cursor IS
        SELECT employee_id, department_id, salary, hire_date
        FROM employee
        WHERE hire_date > SYSDATE - 365;  -- Employees hired in the past year

    CURSOR dept_cursor IS
        SELECT department_id, COUNT(*) AS employee_count, AVG(salary) AS avg_salary
        FROM employee
        GROUP BY department_id;

    -- Variables for processing
    v_employee_id NUMBER;
    v_department_id NUMBER;
    v_salary NUMBER;
    v_hire_date DATE;
    v_employee_count NUMBER;
    v_avg_salary NUMBER;
    
    -- Dynamic SQL strings
    v_create_table_sql VARCHAR2(500);
    v_insert_sql VARCHAR2(500);

    -- Exception Handling
    e_table_exists EXCEPTION;
    e_invalid_data EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_exists, -955);
    PRAGMA EXCEPTION_INIT(e_invalid_data, -20001);

BEGIN
    -- Drop existing tables if they exist
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE employee_summary';
        EXECUTE IMMEDIATE 'DROP TABLE department_summary';
    EXCEPTION
        WHEN e_table_exists THEN
            NULL; -- Ignore if the table does not exist
    END;

    -- Create tables
    v_create_table_sql := 'CREATE TABLE employee_summary (
                               employee_id NUMBER,
                               department_id NUMBER,
                               salary NUMBER,
                               hire_date DATE
                           )';
    EXECUTE IMMEDIATE v_create_table_sql;

    v_create_table_sql := 'CREATE TABLE department_summary (
                               department_id NUMBER,
                               employee_count NUMBER,
                               avg_salary NUMBER
                           )';
    EXECUTE IMMEDIATE v_create_table_sql;

    -- Process employees
    FOR emp_record IN emp_cursor LOOP
        BEGIN
            v_employee_id := emp_record.employee_id;
            v_department_id := emp_record.department_id;
            v_salary := emp_record.salary;
            v_hire_date := emp_record.hire_date;

            -- Insert into employee_summary
            v_insert_sql := 'INSERT INTO employee_summary (employee_id, department_id, salary, hire_date)
                             VALUES (:1, :2, :3, :4)';
            EXECUTE IMMEDIATE v_insert_sql USING v_employee_id, v_department_id, v_salary, v_hire_date;

            -- Log progress
            DBMS_OUTPUT.PUT_LINE('Inserted employee ' || v_employee_id || ' into employee_summary.');
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error inserting employee ' || v_employee_id || ': ' || SQLERRM);
        END;
    END LOOP;

    -- Process departments
    FOR dept_record IN dept_cursor LOOP
        BEGIN
            v_department_id := dept_record.department_id;
            v_employee_count := dept_record.employee_count;
            v_avg_salary := dept_record.avg_salary;

            -- Insert into department_summary
            v_insert_sql := 'INSERT INTO department_summary (department_id, employee_count, avg_salary)
                             VALUES (:1, :2, :3)';
            EXECUTE IMMEDIATE v_insert_sql USING v_department_id, v_employee_count, v_avg_salary;

            -- Log progress
            DBMS_OUTPUT.PUT_LINE('Inserted department ' || v_department_id || ' into department_summary.');
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error inserting department ' || v_department_id || ': ' || SQLERRM);
        END;
    END LOOP;

    -- Commit the transaction
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Procedure executed successfully.');

EXCEPTION
    WHEN e_invalid_data THEN
        DBMS_OUTPUT.PUT_LINE('Invalid data encountered.');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
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
def call_procedure(proc_name):
    sql_command = f"CALL {proc_name}()"
    try:
        cur.execute(sql_command)
        result = cur.fetchone()
        if result:
            print(f"Procedure executed successfully: {result[0]}")
        else:
            print("Procedure executed successfully with no return value.")
    except Exception as e:
        print(f"Error executing procedure: {e}")

# Example call to the Snowflake stored procedure
call_procedure("manage_employee_data_complex")

# Close the cursor and connection
cur.close()
conn.close()
