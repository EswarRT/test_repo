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
SET SERVEROUTPUT ON;
CREATE OR REPLACE PROCEDURE generate_advanced_sales_report (
    p_start_date IN DATE,
    p_end_date IN DATE,
    p_drink_type IN VARCHAR2
)
AS
BEGIN
    -- Initialize debugging
    DBMS_OUTPUT.PUT_LINE('Generating advanced sales report...');
    DBMS_OUTPUT.PUT_LINE('Report Parameters:');
    DBMS_OUTPUT.PUT_LINE('Start Date: ' || TO_CHAR(p_start_date, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('End Date: ' || TO_CHAR(p_end_date, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('Drink Type: ' || p_drink_type);
    DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
    
    -- Raw Sales Data
    DBMS_OUTPUT.PUT_LINE('Raw Sales Data:');
    FOR rec IN (
        SELECT
            sale_date,
            drink_type,
            quantity_sold,
            unit_price,
            (quantity_sold * unit_price) AS total_sale
        FROM soft_drink_sales
        WHERE sale_date BETWEEN p_start_date AND p_end_date
          AND drink_type = p_drink_type
    )
    LOOP
        DBMS_OUTPUT.PUT_LINE('Sale Date: ' || TO_CHAR(rec.sale_date, 'YYYY-MM-DD') ||
                             ', Drink Type: ' || rec.drink_type ||
                             ', Quantity Sold: ' || rec.quantity_sold ||
                             ', Unit Price: ' || rec.unit_price ||
                             ', Total Sale: ' || rec.total_sale);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
    
    -- Daily Summary
    DBMS_OUTPUT.PUT_LINE('Daily Sales Summary:');
    FOR rec IN (
        WITH daily_sales AS (
            SELECT
                sale_date,
                (quantity_sold * unit_price) AS total_sale
            FROM soft_drink_sales
            WHERE sale_date BETWEEN p_start_date AND p_end_date
              AND drink_type = p_drink_type
        )
        SELECT
            sale_date,
            SUM(total_sale) AS daily_total,
            COUNT(*) AS daily_count,
            MAX(total_sale) AS max_daily_sale,
            MIN(total_sale) AS min_daily_sale
        FROM daily_sales
        GROUP BY sale_date
    )
    LOOP
        DBMS_OUTPUT.PUT_LINE('Sale Date: ' || TO_CHAR(rec.sale_date, 'YYYY-MM-DD') ||
                             ', Daily Total: ' || rec.daily_total ||
                             ', Daily Count: ' || rec.daily_count ||
                             ', Max Daily Sale: ' || rec.max_daily_sale ||
                             ', Min Daily Sale: ' || rec.min_daily_sale);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
    
    -- Monthly Summary
    DBMS_OUTPUT.PUT_LINE('Monthly Sales Summary:');
    FOR rec IN (
        WITH monthly_sales AS (
            SELECT
                TO_CHAR(sale_date, 'YYYY-MM') AS month,
                (quantity_sold * unit_price) AS total_sale
            FROM soft_drink_sales
            WHERE sale_date BETWEEN p_start_date AND p_end_date
              AND drink_type = p_drink_type
        )
        SELECT
            month,
            SUM(total_sale) AS monthly_total,
            AVG(total_sale) AS avg_daily_total,
            MAX(total_sale) AS max_monthly_sale,
            MIN(total_sale) AS min_monthly_sale
        FROM monthly_sales
        GROUP BY month
    )
    LOOP
        DBMS_OUTPUT.PUT_LINE('Month: ' || rec.month ||
                             ', Monthly Total: ' || rec.monthly_total ||
                             ', Average Daily Total: ' || rec.avg_daily_total ||
                             ', Max Monthly Sale: ' || rec.max_monthly_sale ||
                             ', Min Monthly Sale: ' || rec.min_monthly_sale);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
    
    -- Cumulative Sales
    DBMS_OUTPUT.PUT_LINE('Cumulative Sales Data:');
    FOR rec IN (
        WITH cumulative_sales AS (
            SELECT
                sale_date,
                (quantity_sold * unit_price) AS total_sale
            FROM soft_drink_sales
            WHERE sale_date BETWEEN p_start_date AND p_end_date
              AND drink_type = p_drink_type
        )
        SELECT
            sale_date,
            SUM(total_sale) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_total
        FROM cumulative_sales
    )
    LOOP
        DBMS_OUTPUT.PUT_LINE('Sale Date: ' || TO_CHAR(rec.sale_date, 'YYYY-MM-DD') ||
                             ', Cumulative Total: ' || rec.cumulative_total);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
    
    -- Ranked Sales
    DBMS_OUTPUT.PUT_LINE('Ranked Sales Data:');
    FOR rec IN (
        WITH ranked_sales AS (
            SELECT
                sale_date,
                drink_type,
                quantity_sold,
                unit_price,
                (quantity_sold * unit_price) AS total_sale,
                ROW_NUMBER() OVER (PARTITION BY sale_date ORDER BY (quantity_sold * unit_price) DESC) AS sale_rank
            FROM soft_drink_sales
            WHERE sale_date BETWEEN p_start_date AND p_end_date
              AND drink_type = p_drink_type
        )
        SELECT
            sale_date,
            drink_type,
            quantity_sold,
            unit_price,
            total_sale,
            sale_rank
        FROM ranked_sales
    )
    LOOP
        DBMS_OUTPUT.PUT_LINE('Sale Date: ' || TO_CHAR(rec.sale_date, 'YYYY-MM-DD') ||
                             ', Drink Type: ' || rec.drink_type ||
                             ', Quantity Sold: ' || rec.quantity_sold ||
                             ', Unit Price: ' || rec.unit_price ||
                             ', Total Sale: ' || rec.total_sale ||
                             ', Sale Rank: ' || rec.sale_rank);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
    
    DBMS_OUTPUT.PUT_LINE('Report generation completed.');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
END generate_advanced_sales_report;
/

BEGIN
    generate_advanced_sales_report(
        p_start_date => TO_DATE('2024-01-01', 'YYYY-MM-DD'),
        p_end_date => TO_DATE('2024-12-31', 'YYYY-MM-DD'),
        p_drink_type => 'Soda'
    );
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

 


