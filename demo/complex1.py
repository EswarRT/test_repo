import re
import google.generativeai as genai


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
* The executed code must provide the output once only instead of repeating twice
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


# Example Oracle procedure
oracle_procedure = """
CREATE OR REPLACE PROCEDURE manage_and_analyze_covid_data(
    p_operation IN VARCHAR2,
    p_start_date IN DATE DEFAULT NULL,
    p_end_date IN DATE DEFAULT NULL,
    p_country IN VARCHAR2 DEFAULT NULL,
    p_state IN VARCHAR2 DEFAULT NULL,
    p_city IN VARCHAR2 DEFAULT NULL,
    p_cases IN NUMBER DEFAULT NULL,
    p_deaths IN NUMBER DEFAULT NULL,
    p_recovered IN NUMBER DEFAULT NULL,
    p_population IN NUMBER DEFAULT NULL,
    p_min_population IN NUMBER DEFAULT NULL,
    p_results OUT SYS_REFCURSOR
) AS
    v_sql VARCHAR2(1000);
BEGIN
    IF p_operation = 'INSERT' THEN
        v_sql := 'INSERT INTO covid_data (country, state, city, report_date, cases, deaths, recovered, population) VALUES (:country, :state, :city, SYSDATE, :cases, :deaths, :recovered, :population)';
        
        EXECUTE IMMEDIATE v_sql USING p_country, p_state, p_city, p_cases, p_deaths, p_recovered, p_population;
        
        DBMS_OUTPUT.PUT_LINE('Insert completed.');

    ELSIF p_operation = 'UPDATE' THEN
        v_sql := 'UPDATE covid_data SET cases = :cases, deaths = :deaths, recovered = :recovered, population = :population WHERE country = :country AND state = :state AND city = :city AND report_date BETWEEN :start_date AND :end_date';
        
        EXECUTE IMMEDIATE v_sql USING p_cases, p_deaths, p_recovered, p_population, p_country, p_state, p_city, p_start_date, p_end_date;
        
        DBMS_OUTPUT.PUT_LINE('Update completed.');

    ELSIF p_operation = 'DELETE' THEN
        v_sql := 'DELETE FROM covid_data WHERE report_date < :cutoff_date';
        
        EXECUTE IMMEDIATE v_sql USING p_start_date;
        
        DBMS_OUTPUT.PUT_LINE('Delete completed.');

    ELSIF p_operation = 'ANALYZE' THEN
        OPEN p_results FOR
        WITH summary AS (
            SELECT
                country,
                state,
                SUM(cases) AS total_cases,
                SUM(deaths) AS total_deaths,
                SUM(recovered) AS total_recovered,
                AVG(cases) AS avg_daily_cases,
                AVG(deaths) AS avg_daily_deaths,
                AVG(recovered) AS avg_daily_recovered,
                SUM(population) AS total_population
            FROM covid_data
            WHERE report_date BETWEEN p_start_date AND p_end_date
            AND (p_min_population IS NULL OR population >= p_min_population)
            GROUP BY country, state
        ),
        rates AS (
            SELECT
                country,
                state,
                total_cases,
                total_deaths,
                total_recovered,
                CASE WHEN total_cases > 0 THEN (total_deaths / total_cases) * 100 ELSE 0 END AS case_fatality_rate,
                CASE WHEN total_cases > 0 THEN (total_recovered / total_cases) * 100 ELSE 0 END AS recovery_rate,
                avg_daily_cases,
                avg_daily_deaths,
                avg_daily_recovered
            FROM summary
        )
        SELECT
            country,
            state,
            total_cases,
            total_deaths,
            total_recovered,
            case_fatality_rate,
            recovery_rate,
            avg_daily_cases,
            avg_daily_deaths,
            avg_daily_recovered
        FROM rates
        ORDER BY country, state;

        DBMS_OUTPUT.PUT_LINE('Analysis completed.');

    ELSE
        RAISE_APPLICATION_ERROR(-20001, 'Invalid operation specified.');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        ROLLBACK;
END;
/


DECLARE
    v_dummy_cursor SYS_REFCURSOR; -- Dummy variable for OUT parameter
BEGIN
    -- INSERT operation
    manage_and_analyze_covid_data(
        p_operation => 'INSERT',
        p_country => 'TestCountry',
        p_state => 'TestState',
        p_city => 'TestCity',
        p_cases => 100,
        p_deaths => 5,
        p_recovered => 50,
        p_population => 500000,
        p_results => v_dummy_cursor -- Pass the dummy cursor
    );
    DBMS_OUTPUT.PUT_LINE('Insert operation completed.');

    -- UPDATE operation
    manage_and_analyze_covid_data(
        p_operation => 'UPDATE',
        p_start_date => SYSDATE - 30,
        p_end_date => SYSDATE,
        p_country => 'TestCountry',
        p_state => 'TestState',
        p_city => 'TestCity',
        p_cases => 120,
        p_deaths => 6,
        p_recovered => 60,
        p_population => 600000,
        p_results => v_dummy_cursor -- Pass the dummy cursor
    );
    DBMS_OUTPUT.PUT_LINE('Update operation completed.');

    -- DELETE operation
    manage_and_analyze_covid_data(
        p_operation => 'DELETE',
        p_start_date => SYSDATE - 60,
        p_results => v_dummy_cursor -- Pass the dummy cursor
    );
    DBMS_OUTPUT.PUT_LINE('Delete operation completed.');
END;
/


DECLARE
    v_cursor SYS_REFCURSOR;
    v_country covid_data1.country%TYPE;
    v_state covid_data1.state%TYPE;
    v_total_cases covid_data1.cases%TYPE;
    v_total_deaths covid_data1.deaths%TYPE;
    v_total_recovered covid_data1.recovered%TYPE;
    v_case_fatality_rate NUMBER;
    v_recovery_rate NUMBER;
    v_avg_daily_cases covid_data1.cases%TYPE;
    v_avg_daily_deaths covid_data1.deaths%TYPE;
    v_avg_daily_recovered covid_data1.recovered%TYPE;
BEGIN
    -- Call the stored procedure with a date range and minimum population
    manage_and_analyze_covid_data1(
        p_operation => 'ANALYZE',
        p_start_date => SYSDATE - 30,
        p_end_date => SYSDATE,
        p_min_population => 500000,
        p_results => v_cursor -- Use a variable to capture the output
    );
    
    -- Fetch and display results from the cursor
    LOOP
        FETCH v_cursor INTO v_country, v_state, v_total_cases, v_total_deaths, v_total_recovered,
            v_case_fatality_rate, v_recovery_rate, v_avg_daily_cases, v_avg_daily_deaths, v_avg_daily_recovered;
        EXIT WHEN v_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('Country: ' || v_country ||
                             ', State: ' || v_state ||
                             ', Total Cases: ' || v_total_cases ||
                             ', Total Deaths: ' || v_total_deaths ||
                             ', Total Recovered: ' || v_total_recovered ||
                             ', Case Fatality Rate: ' || v_case_fatality_rate ||
                             ', Recovery Rate: ' || v_recovery_rate ||
                             ', Avg Daily Cases: ' || v_avg_daily_cases ||
                             ', Avg Daily Deaths: ' || v_avg_daily_deaths ||
                             ', Avg Daily Recovered: ' || v_avg_daily_recovered);
    END LOOP;
    
    CLOSE v_cursor;
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

# snowflake conection lenidi 


# Orcl lan@123