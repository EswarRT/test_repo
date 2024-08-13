import re
import snowflake.connector

# Function to convert Oracle PL/SQL to Snowflake JavaScript stored procedure
def convert_oracle_to_snowflake(oracle_procedure):
    # Define a simple mapping of Oracle to Snowflake data types and constructs
    type_mapping = {
        'NUMBER': 'FLOAT',
        'VARCHAR2': 'STRING',
        'DATE': 'STRING'  # Use STRING and convert to DATE in JavaScript
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
            param_type = param_parts[2]
            param_type = type_mapping.get(param_type.upper(), param_type)
            param_list.append(f'{param_name} {param_type}')
    snowflake_parameters = ', '.join(param_list)
    
    # Extract the body of the procedure
    body_start = oracle_procedure.index("BEGIN") + len("BEGIN")
    body_end = oracle_procedure.rindex("END")  # Use rindex to find the last occurrence of "END"
    procedure_body = oracle_procedure[body_start:body_end].strip()
    
    # Replace Oracle SQL functions with Snowflake SQL equivalents
    procedure_body = procedure_body.replace("SYSDATE", "CURRENT_TIMESTAMP")
    procedure_body = procedure_body.replace("DBMS_OUTPUT.PUT_LINE", "log_messages.push")  # Simple replacement for demonstration

    # Remove COMMIT statements (Snowflake auto-commits)
    procedure_body = re.sub(r'\bCOMMIT\b;', '', procedure_body, flags=re.IGNORECASE)
    
    # Generate Snowflake stored procedure in JavaScript
    snowflake_procedure = f"""
CREATE OR REPLACE PROCEDURE {proc_name}({snowflake_parameters})
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var v_total_fee = 0;
var v_discount = 0;
var v_final_fee = 0;
var v_student_balance = 0;
var v_grade_level = '';
var v_course_fee = 0;
var v_available_slots = 0;
var log_messages = [];

try {{
    // Get the course fee and available slots
    var sql_command = `SELECT FEE, AVAILABLE_SLOTS
                       FROM COURSES
                       WHERE COURSE_ID = :1`;
    var result = snowflake.execute({{sqlText: sql_command, binds: [p_course_id]}});
    if (result.next()) {{
        v_course_fee = result.getColumnValue('FEE');
        v_available_slots = result.getColumnValue('AVAILABLE_SLOTS');
    }}

    // Check if there are available slots
    if (v_available_slots <= 0) {{
        throw new Error('No available slots for course ID ' + p_course_id);
    }}

    // Calculate the total fee
    v_total_fee = v_course_fee;

    // Get student details
    sql_command = `SELECT BALANCE, GRADE_LEVEL
                   FROM STUDENTS
                   WHERE STUDENT_ID = :1`;
    result = snowflake.execute({{sqlText: sql_command, binds: [p_student_id]}});
    if (result.next()) {{
        v_student_balance = result.getColumnValue('BALANCE');
        v_grade_level = result.getColumnValue('GRADE_LEVEL');
    }}

    // Apply discounts based on grade level
    if (v_grade_level === 'Junior') {{
        v_discount = v_total_fee * 0.1; // 10% discount for Juniors
    }} else if (v_grade_level === 'Senior') {{
        v_discount = v_total_fee * 0.2; // 20% discount for Seniors
    }} else {{
        v_discount = 0; // No discount for Freshmen and Sophomores
    }}

    v_final_fee = v_total_fee - v_discount;

    // Check if student has sufficient balance
    if (v_student_balance < v_final_fee) {{
        throw new Error('Insufficient balance for student ID ' + p_student_id);
    }}

    // Update the available slots
    sql_command = `UPDATE COURSES
                   SET AVAILABLE_SLOTS = AVAILABLE_SLOTS - 1
                   WHERE COURSE_ID = :1`;
    snowflake.execute({{sqlText: sql_command, binds: [p_course_id]}});

    // Insert a new enrollment
    sql_command = `INSERT INTO ENROLLMENTS (ENROLLMENT_ID, STUDENT_ID, COURSE_ID, ENROLLMENT_DATE, FEE_PAID, DISCOUNT_APPLIED, ENROLLMENT_STATUS)
                   VALUES (:1, :2, :3, :4, :5, :6, 'Confirmed')`;
    snowflake.execute({{
        sqlText: sql_command,
        binds: [p_enrollment_id, p_student_id, p_course_id, p_enrollment_date, v_final_fee, v_discount]
    }});

    // Update student's balance
    sql_command = `UPDATE STUDENTS
                   SET BALANCE = BALANCE - :1
                   WHERE STUDENT_ID = :2`;
    snowflake.execute({{sqlText: sql_command, binds: [v_final_fee, p_student_id]}});

    // Set the OUT parameter
    p_fee_paid = v_final_fee;

    return 'process_enrollment executed successfully';
}} catch (err) {{
    return 'An error occurred: ' + err.message;
}}
$$;
"""
    return snowflake_procedure

# Example Oracle procedure
oracle_procedure = """
CREATE OR REPLACE PROCEDURE process_enrollment (
    p_enrollment_id  IN NUMBER,
    p_student_id     IN NUMBER,
    p_course_id      IN NUMBER,
    p_enrollment_date IN DATE,
    p_fee_paid       OUT NUMBER
)
IS
    v_total_fee      NUMBER := 0;
    v_discount       NUMBER := 0;
    v_final_fee      NUMBER := 0;
    v_student_balance NUMBER;
    v_grade_level    VARCHAR2(20);
    v_course_fee     NUMBER;
    v_available_slots NUMBER;
BEGIN
    -- Start a transaction
    SAVEPOINT before_enrollment;

    -- Get the course fee and available slots
    SELECT fee, available_slots
    INTO v_course_fee, v_available_slots
    FROM courses
    WHERE course_id = p_course_id;

    -- Check if there are available slots
    IF v_available_slots <= 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'No available slots for course ID ' || p_course_id);
    END IF;

    -- Calculate the total fee
    v_total_fee := v_course_fee;

    -- Get student details
    SELECT balance, grade_level
    INTO v_student_balance, v_grade_level
    FROM students
    WHERE student_id = p_student_id;

    -- Apply discounts based on grade level
    IF v_grade_level = 'Junior' THEN
        v_discount := v_total_fee * 0.1; -- 10% discount for Juniors
    ELSIF v_grade_level = 'Senior' THEN
        v_discount := v_total_fee * 0.2; -- 20% discount for Seniors
    ELSE
        v_discount := 0; -- No discount for Freshmen and Sophomores
    END IF;

    v_final_fee := v_total_fee - v_discount;

    -- Check if student has sufficient balance
    IF v_student_balance < v_final_fee THEN
        RAISE_APPLICATION_ERROR(-20002, 'Insufficient balance for student ID ' || p_student_id);
    END IF;

    -- Update the available slots
    UPDATE courses
    SET available_slots = available_slots - 1
    WHERE course_id = p_course_id;

    -- Insert a new enrollment
    INSERT INTO enrollments (enrollment_id, student_id, course_id, enrollment_date, fee_paid, discount_applied, enrollment_status)
    VALUES (p_enrollment_id, p_student_id, p_course_id, p_enrollment_date, v_final_fee, v_discount, 'Confirmed');

    -- Update student's balance
    UPDATE students
    SET balance = balance - v_final_fee
    WHERE student_id = p_student_id;

    -- Set the OUT parameter
    p_fee_paid := v_final_fee;

    -- Commit the transaction
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        -- Rollback to the savepoint in case of any error
        ROLLBACK TO before_enrollment;
        -- Raise the exception
        RAISE;
END;
/
"""

# Convert Oracle procedure to Snowflake procedure
snowflake_procedure = convert_oracle_to_snowflake(oracle_procedure)
print(snowflake_procedure)

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

# Create the TEMP_LOGS table if it doesn't exist
# execute_sql("CREATE TABLE IF NOT EXISTS TEMP_LOGS (LOG_MESSAGE STRING)")

# Execute the converted Snowflake procedure
execute_sql(snowflake_procedure)

# Close the cursor and connection
cur.close()
conn.close()
#not working