# import re
# import google.generativeai as genai


# # Configure the Generative AI model
# genai.configure(api_key='AIzaSyCqwFedZfvnT90Oe37_dZZShdf0JRmDYck')
# model = genai.GenerativeModel(model_name="gemini-1.5-pro-001")

# # Define the initial part of the prompt for Generative AI
# initial_prompt = """
# You are a Oracle SQL, Snowflake and Python code expert.

# I will provide you with a Oracle Query. Please convert it to Snowflake SQL Query, adhering to the following guidelines:

# 1. Deep Understanding:
# * Analyze the Oracle SQL query for its syntax, semantics, data types, and logical flow.
# * Understand the Snowflake Syntax, semantics, data types and logical flow.
# * Understand which component of Snowflake is supported for Oracle components.
# 2. Accurate Snowflake Conversion:
# * Convert the Oracle SQL query into a Snowflake SQL query.
# * Ensure all functionalities are preserved.
# * Adjust Oracle-specific components and syntax to their Snowflake equivalents.
# * Pay attention to data types, function calls, PL/SQL constructs, and procedural elements.
# * Ensure the converted query is compatible with Snowflake’s SQL syntax and features.
# * Verify schema and table structures in Snowflake align with Oracle’s schema.
# * Convert and test functions, procedures, and other database objects.
# * Migrate user permissions and security settings as needed by making them compatible with Snowflake.
# * If the permissions require to be done in Snowflake then make sure to notify them in the form of comments.
# * If the comments are in JavaScript then use JavaScript format.
# * Adjust numeric data types for precision and scale.
# * Confirm and convert date and time functions as necessary.
# * The date and time functions should be supported in Snowflake. 
# * If the query contains SYSDATE make sure to convert it into Snowflake Supported Function.
# * Optimize queries for Snowflake’s architecture.
# * Convert Oracle’s exception handling to a Snowflake-compatible approach.
# * Ensure error messages and diagnostics are suitable for Snowflake.
# * Adapt Oracle’s transaction control commands to Snowflake’s transactional behavior.
# * Convert CONNECT BY hierarchical queries to Snowflake’s recursive CTEs if applicable.
# * Remove or convert SQL*Plus-specific commands (e.g., SPOOL, DEFINE).
# * Translate Oracle-specific functions or create custom UDFs for Snowflake.
# * Must include the return type in Snowflake SQL Query when converting the Oracle SQL Query into Snowflake for the procedures.
# * Must and should use JavaScript for procedural logic and cursor handling in Snowflake.
# * When using JavaScript return data in Variant or Varchar type based on reqiurement.
# * Do not return table in stored procedure as Snowflakedoes not support it.
# * If there is more than one operation to be performed in the oracle stored procedure then create a seperate and individual stored procedure in Snowflake for every operation present in the stored procedure without fail. 
# * Convert funtions present in the Oracle stored procedure into Snowflake stored procedures.
# * While using JavaScript if the input Oracle query has a data type Number(a,b) format while converting use Float data type for Snowflake SQL Query.
# * Only use FLOAT instead of Number(a,b) in stored procedure creation not the normal table creation.
# * Make sure to use the Insert INTO statements correctly and it must and should follow and support Snowflake Syntax.
# * Prioritise using only JavaScript Datatypes while using JavaScript for stored procedure.
# * Make sure the procedure parameters are referred correctly in JavaScript.
# * Bind the parameters used while creating the stored procedure properly using the cursor so that we will not get the parameter_name not defined while calling the function.
# * While binding the Date datatype parameters bind using toISOString() so as to not get date not matching error when wea re calling the function.
# * Make sure to use only correct datatypes while converting.
# * In the stored procedure make sure that all the components are declared correctly along with their data types so that when we call the procedure we can perform the operations without facing any errors.
# * Must and should work for all stored procedure logics.
# * Convert entire Oracle query into snowflake sql query.
# * For triggers in Oracle Query use a correct and supported mechanism when converting into Snowflake SQL Query because trigger is not supporeted in Snowflake.
# * Use Sequence to generate a sequence in Snowflake while converting.
# * Data must be loaded into the table correctly without fail.
# * If there are loops in Oracle convert them into Snowflake SQL loop using JavaScript inside the stored procedure rather than outside the procedure.
# * Check for any syntax, logical errors while converting the loop and give the correct query.
# * Use only FOR, While, Repeat loops while converting loop from Oracle query into Snowflake query.
# * Make sure to check the entire query and write the looping statements correctly according to Snowflake SQL.
# * The data in the loops needed to be inserted correctly.
# * Carefully check whether the components are supported in Snowflake or not while converting the code ane make sure to use only supported components.
# * The convered code should only have components that are supported by Snowflake.
# * The stored procedure must be correct and work properly.
# * Use correct binding parameters while using parameter binding and for binding in the execute statement.
# * While calling the Stored Procedures in the Converted code make sure to follow Snowflake Syntax guidelines.
# * Must use correct calling method while calling the stored procedure.
# * Make sure the calling function should return the correct operation we specified and not other operations.
# * Use correct case if using JavaScript because it is case-sensitive.
# * Make sure to the insert statements in Snowflake query should only follow the snowflake syntax.

# 3. Output:
# * The output should contain only the Snowflake query.
# * The returned query should be executable in Snowflake without errors.
# * The output should be enclosed within triple quotes (""" """).
# * Print the output only once.
# * There is no need to include oracle query in the Snowflake query even in the comments.
# * The executed code must provide the output once only instead of repeating twice
# """

# def convert_oracle_to_snowflake_with_ai(oracle_query):
#     """
#     Convert Oracle SQL query to Snowflake SQL query using Generative AI.
#     """
#     prompt = initial_prompt + "\n\nOracle SQL Query:\n" + oracle_query

#     try:
#         # Generate content using Generative AI
#         response = model.generate_content(prompt)
        
#         if response.candidates and len(response.candidates) > 0:
#             content = response.candidates[0].content
#             text = content.parts[0].text
#             return text
#         else:
#             return 'No content found'
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         return None

# def apply_regex_transformations(snowflake_procedure):
#     """
#     Apply regex-based transformations to the Snowflake procedure code.
#     """
#     # Example regex transformations
#     snowflake_procedure = re.sub(r'\bVARCHAR2\b', 'VARCHAR', snowflake_procedure)
#     snowflake_procedure = re.sub(r'\bNUMBER\((\d+),(\d+)\)\b', 'FLOAT', snowflake_procedure)
#     snowflake_procedure = re.sub(r'CREATE OR REPLACE PROCEDURE\s+(\w+)\s+\((.*?)\)\s+AS', r'CREATE OR REPLACE PROCEDURE \1(\2) RETURNS STRING LANGUAGE JAVASCRIPT AS', snowflake_procedure)

#     # Add any additional regex transformations as needed
#     return snowflake_procedure

# def convert_oracle_procedure(oracle_procedure):
#     """
#     Integrate Generative AI and regex-based transformations for Oracle to Snowflake conversion.
#     """
#     # Step 1: Use Generative AI for initial conversion
#     ai_converted_code = convert_oracle_to_snowflake_with_ai(oracle_procedure)

#     # Step 2: Apply regex-based transformations
#     final_converted_code = apply_regex_transformations(ai_converted_code)

#     return final_converted_code


# # Example Oracle procedure
# oracle_procedure = """
# CREATE OR REPLACE PROCEDURE manage_employee (
#     p_action         IN VARCHAR2,
#     p_employee_id    IN NUMBER DEFAULT NULL,
#     p_first_name     IN VARCHAR2 DEFAULT NULL,
#     p_last_name      IN VARCHAR2 DEFAULT NULL,
#     p_department_id  IN NUMBER DEFAULT NULL,
#     p_salary         IN NUMBER DEFAULT NULL,
#     p_hire_date      IN DATE DEFAULT NULL
# )
# IS
#     -- Declare a local variable to hold the cursor result
#     CURSOR emp_cursor IS
#         SELECT employee_id, first_name, last_name, department_id, salary, hire_date
#         FROM employee
#         WHERE employee_id = p_employee_id;

#     v_emp_record emp_cursor%ROWTYPE;
    
#     -- Declare a variable to hold the dynamic SQL
#     v_sql VARCHAR2(4000);
    
#     -- Exception handling variables
#     e_no_data_found EXCEPTION;
#     e_invalid_action EXCEPTION;
#     e_invalid_salary EXCEPTION;
    
# BEGIN
#     -- Start transaction
#     SAVEPOINT sp_manage_employee;
    
#     -- Action-based processing
#     CASE p_action
#         WHEN 'INSERT' THEN
#             -- Validate inputs
#             IF p_first_name IS NULL OR p_last_name IS NULL OR p_department_id IS NULL OR p_salary IS NULL OR p_hire_date IS NULL THEN
#                 RAISE_APPLICATION_ERROR(-20001, 'All parameters must be provided for INSERT.');
#             END IF;
            
#             -- Check if employee_id already exists
#             OPEN emp_cursor;
#             FETCH emp_cursor INTO v_emp_record;
#             IF emp_cursor%FOUND THEN
#                 RAISE_APPLICATION_ERROR(-20002, 'Employee with this ID already exists.');
#             END IF;
#             CLOSE emp_cursor;
            
#             -- Insert new employee
#             INSERT INTO employee (employee_id, first_name, last_name, department_id, salary, hire_date)
#             VALUES (p_employee_id, p_first_name, p_last_name, p_department_id, p_salary, p_hire_date);
        
#         WHEN 'UPDATE' THEN
#             -- Validate inputs
#             IF p_employee_id IS NULL THEN
#                 RAISE_APPLICATION_ERROR(-20003, 'Employee ID must be provided for UPDATE.');
#             END IF;
            
#             -- Check if employee exists
#             OPEN emp_cursor;
#             FETCH emp_cursor INTO v_emp_record;
#             IF NOT emp_cursor%FOUND THEN
#                 RAISE_APPLICATION_ERROR(-20004, 'Employee with this ID does not exist.');
#             END IF;
#             CLOSE emp_cursor;
            
#             -- Update employee record
#             v_sql := 'UPDATE employee SET first_name = :1, last_name = :2, department_id = :3, salary = :4, hire_date = :5 WHERE employee_id = :6';
#             EXECUTE IMMEDIATE v_sql USING p_first_name, p_last_name, p_department_id, p_salary, p_hire_date, p_employee_id;
        
#         WHEN 'DELETE' THEN
#             -- Validate inputs
#             IF p_employee_id IS NULL THEN
#                 RAISE_APPLICATION_ERROR(-20005, 'Employee ID must be provided for DELETE.');
#             END IF;
            
#             -- Check if employee exists
#             OPEN emp_cursor;
#             FETCH emp_cursor INTO v_emp_record;
#             IF NOT emp_cursor%FOUND THEN
#                 RAISE_APPLICATION_ERROR(-20006, 'Employee with this ID does not exist.');
#             END IF;
#             CLOSE emp_cursor;
            
#             -- Delete employee record
#             DELETE FROM employee WHERE employee_id = p_employee_id;
        
#         WHEN 'RETRIEVE' THEN
#             -- Retrieve employee details
#             OPEN emp_cursor;
#             FETCH emp_cursor INTO v_emp_record;
#             IF emp_cursor%FOUND THEN
#                 DBMS_OUTPUT.PUT_LINE('Employee ID: ' || v_emp_record.employee_id);
#                 DBMS_OUTPUT.PUT_LINE('First Name: ' || v_emp_record.first_name);
#                 DBMS_OUTPUT.PUT_LINE('Last Name: ' || v_emp_record.last_name);
#                 DBMS_OUTPUT.PUT_LINE('Department ID: ' || v_emp_record.department_id);
#                 DBMS_OUTPUT.PUT_LINE('Salary: ' || v_emp_record.salary);
#                 DBMS_OUTPUT.PUT_LINE('Hire Date: ' || TO_CHAR(v_emp_record.hire_date, 'YYYY-MM-DD'));
#             ELSE
#                 DBMS_OUTPUT.PUT_LINE('No employee found with ID ' || p_employee_id);
#             END IF;
#             CLOSE emp_cursor;
        
#         ELSE
#             RAISE e_invalid_action;
#     END CASE;
    
#     -- Commit the transaction
#     COMMIT;
    
# EXCEPTION
#     WHEN NO_DATA_FOUND THEN
#         -- Handle the case when no data is found
#         ROLLBACK TO sp_manage_employee;
#         DBMS_OUTPUT.PUT_LINE('No data found.');
    
#     WHEN e_invalid_action THEN
#         -- Handle invalid action error
#         ROLLBACK TO sp_manage_employee;
#         DBMS_OUTPUT.PUT_LINE('Invalid action specified.');
    
#     WHEN e_invalid_salary THEN
#         -- Handle invalid salary error
#         ROLLBACK TO sp_manage_employee;
#         DBMS_OUTPUT.PUT_LINE('Invalid salary amount provided.');
    
#     WHEN OTHERS THEN
#         -- Handle other exceptions
#         ROLLBACK TO sp_manage_employee;
#         DBMS_OUTPUT.PUT_LINE('An unexpected error occurred: ' || SQLERRM);
    
# END manage_employee;
# /
# """

# # Convert Oracle procedure to Snowflake procedure
# try:
#     snowflake_procedure = convert_oracle_procedure(oracle_procedure)
#     print("Snowflake SQL Query:")
#     print(snowflake_procedure)
# except ValueError as e:
#     print(e)

# # snowflake conection lenidi 
# ----------------------------------------------------------
import re
import google.generativeai as genai
import snowflake.connector

# Configure the Generative AI model
genai.configure(api_key='AIzaSyCqwFedZfvnT90Oe37_dZZShdf0JRmDYck')
model = genai.GenerativeModel(model_name="gemini-1.5-pro-001")

# Define the initial part of the prompt for Generative AI
initial_prompt = """
You are a Oracle SQL, Snowflake and Python code expert.

I will provide you with a Oracle Query. Please convert it to Snowflake SQL Query, adhering to the following guidelines:

1. Deep Understanding:
* Analyze the Oracle SQL query for its syntax, semantics, data types, and logical flow.
* Understand the Snowflake Syntax, semantics, data types and logical flow.
* Understand which component of Snowflake is supported for Oracle components.
2. Accurate Snowflake Conversion:
* Convert the Oracle SQL query into a Snowflake SQL query.
* Ensure all functionalities are preserved.
* Adjust Oracle-specific components and syntax to their Snowflake equivalents.
* Pay attention to data types, function calls, PL/SQL constructs, and procedural elements.
* Ensure the converted query is compatible with Snowflake’s SQL syntax and features.
* Verify schema and table structures in Snowflake align with Oracle’s schema.
* Convert and test functions, procedures, and other database objects.
* Migrate user permissions and security settings as needed by making them compatible with Snowflake.
* If the permissions require to be done in Snowflake then make sure to notify them in the form of comments.
* If the comments are in JavaScript then use JavaScript format.
* Adjust numeric data types for precision and scale.
* Confirm and convert date and time functions as necessary.
* The date and time functions should be supported in Snowflake. 
* If the query contains SYSDATE make sure to convert it into Snowflake Supported Function.
* Optimize queries for Snowflake’s architecture.
* Convert Oracle’s exception handling to a Snowflake-compatible approach.
* Ensure error messages and diagnostics are suitable for Snowflake.
* Adapt Oracle’s transaction control commands to Snowflake’s transactional behavior.
* Convert CONNECT BY hierarchical queries to Snowflake’s recursive CTEs if applicable.
* Remove or convert SQL*Plus-specific commands (e.g., SPOOL, DEFINE).
* Translate Oracle-specific functions or create custom UDFs for Snowflake.
* Must include the return type in Snowflake SQL Query when converting the Oracle SQL Query into Snowflake for the procedures.
* Must and should use JavaScript for procedural logic and cursor handling in Snowflake.
* When using JavaScript return data in Variant or Varchar type based on reqiurement.
* Do not return table in stored procedure as Snowflakedoes not support it.
* If there is more than one operation to be performed in the oracle stored procedure then create a seperate and individual stored procedure in Snowflake for every operation present in the stored procedure without fail. 
* Convert funtions present in the Oracle stored procedure into Snowflake stored procedures.
* While using JavaScript if the input Oracle query has a data type Number(a,b) format while converting use Float data type for Snowflake SQL Query.
* Only use FLOAT instead of Number(a,b) in stored procedure creation not the normal table creation.
* Make sure to use the Insert INTO statements correctly and it must and should follow and support Snowflake Syntax.
* Prioritise using only JavaScript Datatypes while using JavaScript for stored procedure.
* Make sure the procedure parameters are referred correctly in JavaScript.
* Bind the parameters used while creating the stored procedure properly using the cursor so that we will not get the parameter_name not defined while calling the function.
* While binding the Date datatype parameters bind using toISOString() so as to not get date not matching error when wea re calling the function.
* Make sure to use only correct datatypes while converting.
* In the stored procedure make sure that all the components are declared correctly along with their data types so that when we call the procedure we can perform the operations without facing any errors.
* Must and should work for all stored procedure logics.
* Convert entire Oracle query into snowflake sql query.
* For triggers in Oracle Query use a correct and supported mechanism when converting into Snowflake SQL Query because trigger is not supporeted in Snowflake.
* Use Sequence to generate a sequence in Snowflake while converting.
* Data must be loaded into the table correctly without fail.
* If there are loops in Oracle convert them into Snowflake SQL loop using JavaScript inside the stored procedure rather than outside the procedure.
* Check for any syntax, logical errors while converting the loop and give the correct query.
* Use only FOR, While, Repeat loops while converting loop from Oracle query into Snowflake query.
* Make sure to check the entire query and write the looping statements correctly according to Snowflake SQL.
* The data in the loops needed to be inserted correctly.
* Carefully check whether the components are supported in Snowflake or not while converting the code ane make sure to use only supported components.
* The convered code should only have components that are supported by Snowflake.
* The stored procedure must be correct and work properly.
* Use correct binding parameters while using parameter binding and for binding in the execute statement.
* While calling the Stored Procedures in the Converted code make sure to follow Snowflake Syntax guidelines.
* Must use correct calling method while calling the stored procedure.
* Make sure the calling function should return the correct operation we specified and not other operations.
* Use correct case if using JavaScript because it is case-sensitive.
* Make sure to the insert statements in Snowflake query should only follow the snowflake syntax.

3. Output:
* The output should contain only the Snowflake query.
* The returned query should be executable in Snowflake without errors.
* The output should be enclosed within triple quotes (""" """).
* Print the output only once.
* There is no need to include oracle query in the Snowflake query even in the comments.
"""

def convert_oracle_to_snowflake_with_ai(oracle_query):
    """
    Convert Oracle SQL query to Snowflake SQL query using Generative AI.
    """
    prompt = initial_prompt + "\n\nOracle SQL Query:\n" + oracle_query

    try:
        # Generate content using Generative AI
        response = model.generate_content(prompt)
        
        if response.candidates and len(response.candidates) > 0:
            content = response.candidates[0].content
            text = content.parts[0].text
            return text
        else:
            return 'No content found'
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def apply_regex_transformations(snowflake_procedure):
    """
    Apply regex-based transformations to the Snowflake procedure code.
    """
    # Example regex transformations
    snowflake_procedure = re.sub(r'\bVARCHAR2\b', 'VARCHAR', snowflake_procedure)
    snowflake_procedure = re.sub(r'\bNUMBER\((\d+),(\d+)\)\b', 'FLOAT', snowflake_procedure)
    snowflake_procedure = re.sub(r'CREATE OR REPLACE PROCEDURE\s+(\w+)\s+\((.*?)\)\s+AS', r'CREATE OR REPLACE PROCEDURE \1(\2) RETURNS STRING LANGUAGE JAVASCRIPT AS', snowflake_procedure)

    # Add any additional regex transformations as needed
    return snowflake_procedure

def convert_oracle_procedure(oracle_procedure):
    """
    Integrate Generative AI and regex-based transformations for Oracle to Snowflake conversion.
    """
    # Step 1: Use Generative AI for initial conversion
    ai_converted_code = convert_oracle_to_snowflake_with_ai(oracle_procedure)

    # Step 2: Apply regex-based transformations
    final_converted_code = apply_regex_transformations(ai_converted_code)

    return final_converted_code

def execute_sql(command):
    """
    Execute SQL command on Snowflake.
    """
    try:
        cur.execute(command)
        print(f"Executed: {command}")
    except Exception as e:
        print(f"Error executing {command}: {e}")

def call_procedure(proc_name, *params):
    """
    Call Snowflake stored procedure with parameters.
    """
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
try:
    snowflake_procedure = convert_oracle_procedure(oracle_procedure)
    print("Snowflake SQL Query:")
    print(snowflake_procedure)
except ValueError as e:
    print(e)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='ESWARMANIKANTA',
    password='Eswar@7185',
    account='pt90021.europe-west4.gcp',
    warehouse='COMPUTE_WH',
    database='WASTE',
    schema='PUBLIC'
)

cur = conn.cursor()

# Execute the converted Snowflake procedure
if 'snowflake_procedure' in locals():
    execute_sql(snowflake_procedure)
else:
    print("Snowflake procedure was not generated successfully.")

# Example call to the Snowflake stored procedure


# Close the cursor and connection
cur.close()
conn.close()


# snowflake conection unna daniki sql error vastundi