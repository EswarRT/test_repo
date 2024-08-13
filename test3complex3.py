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
    proc_name_match = re.search(r'PROCEDURE\s+(\w+)\s*IS', oracle_procedure, re.DOTALL)
    if not proc_name_match:
        raise ValueError("The provided Oracle procedure does not match the expected format.")
    
    proc_name = proc_name_match.group(1)
    
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
executeSQL(query);
''', procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'UPDATE\s+(.*?)\s+SET\s+(.*?)\s+WHERE\s+(.*?);', r'''
var query = `UPDATE \1 SET \2 WHERE \3`;
executeSQL(query);
''', procedure_body, flags=re.DOTALL)

    procedure_body = re.sub(r'DELETE FROM\s+(.*?)\s+WHERE\s+(.*?);', r'''
var query = `DELETE FROM \1 WHERE \2`;
executeSQL(query);
''', procedure_body, flags=re.DOTALL)

    # Replace RAISE_APPLICATION_ERROR with throw
    procedure_body = re.sub(r'RAISE_APPLICATION_ERROR\(-\d+, (.+?)\);', r'throw new Error(\1);', procedure_body)

    # Remove comments
    procedure_body = re.sub(r'--.*', '', procedure_body)
    procedure_body = re.sub(r'/\*.*?\*/', '', procedure_body, flags=re.DOTALL)

    # Generate Snowflake stored procedure in JavaScript
    snowflake_procedure = f"""
CREATE OR REPLACE PROCEDURE {proc_name}()
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
    user='Eswarmanikanta',
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

# Execute the procedure call
# execute_sql("CALL manage_employee_data_complex();")

# Close the cursor and connection
cur.close()
conn.close()


