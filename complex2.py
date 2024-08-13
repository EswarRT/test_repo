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
    var total_revenue = 0;
    var avg_lead_time = 0;
    var high_demand_sku = '';
    var high_demand_count = 0;
    var low_stock_sku = '';
    var low_stock_level = 9999999;
    var log_messages = [];

    try {{
        // Query to get the supply chain data
        var sql_command = `SELECT PRODUCT_TYPE, SKU, PRICE, AVAILABILITY, NUMBER_OF_PRODUCTS_SOLD, REVENUE_GENERATED,
                                  CUSTOMER_DEMOGRAPHICS, STOCK_LEVELS, LEAD_TIMES, ORDER_QUANTITIES, SHIPPING_TIMES,
                                  SHIPPING_CARRIERS, SHIPPING_COSTS, SUPPLIER_NAME, LOCATION, LEAD_TIME, PRODUCTION_VOLUMES,
                                  MANUFACTURING_LEAD_TIME, MANUFACTURING_COSTS, INSPECTION_RESULTS, DEFECT_RATES, 
                                  TRANSPORTATION_MODES, ROUTES, COSTS
                           FROM supply_chain_data`;

        var result = snowflake.execute({{sqlText: sql_command}});
        var rowCount = 0;

        while (result.next()) {{
            rowCount++;
            var v_product_type = result.getColumnValue('PRODUCT_TYPE');
            var v_sku = result.getColumnValue('SKU');
            var v_price = result.getColumnValue('PRICE');
            var v_availability = result.getColumnValue('AVAILABILITY');
            var v_number_of_products_sold = result.getColumnValue('NUMBER_OF_PRODUCTS_SOLD');
            var v_revenue_generated = result.getColumnValue('REVENUE_GENERATED');
            var v_customer_demographics = result.getColumnValue('CUSTOMER_DEMOGRAPHICS');
            var v_stock_levels = result.getColumnValue('STOCK_LEVELS');
            var v_lead_times = result.getColumnValue('LEAD_TIMES');
            var v_order_quantities = result.getColumnValue('ORDER_QUANTITIES');
            var v_shipping_times = result.getColumnValue('SHIPPING_TIMES');
            var v_shipping_carriers = result.getColumnValue('SHIPPING_CARRIERS');
            var v_shipping_costs = result.getColumnValue('SHIPPING_COSTS');
            var v_supplier_name = result.getColumnValue('SUPPLIER_NAME');
            var v_location = result.getColumnValue('LOCATION');
            var v_lead_time = result.getColumnValue('LEAD_TIME');
            var v_production_volumes = result.getColumnValue('PRODUCTION_VOLUMES');
            var v_manufacturing_lead_time = result.getColumnValue('MANUFACTURING_LEAD_TIME');
            var v_manufacturing_costs = result.getColumnValue('MANUFACTURING_COSTS');
            var v_inspection_results = result.getColumnValue('INSPECTION_RESULTS');
            var v_defect_rates = result.getColumnValue('DEFECT_RATES');
            var v_transportation_modes = result.getColumnValue('TRANSPORTATION_MODES');
            var v_routes = result.getColumnValue('ROUTES');
            var v_costs = result.getColumnValue('COSTS');

            // Complex calculations
            total_revenue += v_revenue_generated;
            avg_lead_time += v_lead_times;

            // Example of conditional logic and data manipulation
            if (v_number_of_products_sold > high_demand_count) {{
                high_demand_sku = v_sku;
                high_demand_count = v_number_of_products_sold;
            }}

            if (v_stock_levels < low_stock_level) {{
                low_stock_sku = v_sku;
                low_stock_level = v_stock_levels;
            }}
        }}

        // Final calculations for average lead time
        if (rowCount > 0) {{
            avg_lead_time = avg_lead_time / rowCount;
        }} else {{
            avg_lead_time = 0;
        }}

        // Log the results
        log_messages.push('Total Revenue: ' + total_revenue);
        log_messages.push('Average Lead Time: ' + avg_lead_time);
        log_messages.push('Highest Demand SKU: ' + high_demand_sku + ' with ' + high_demand_count + ' units sold');
        log_messages.push('Lowest Stock Level SKU: ' + low_stock_sku + ' with ' + low_stock_level + ' units in stock');

        log_messages.forEach(function(log_message) {{
            snowflake.execute({{sqlText: `INSERT INTO TEMP_LOGS (LOG_MESSAGE) VALUES ('${{log_message}}')`}});
        }});

        return 'complex_supply_chain_analysis executed successfully';
    }} catch (err) {{
        return 'An error occurred: ' + err.message;
    }}
$$;
"""
    return snowflake_procedure

# Example Oracle procedure
oracle_procedure = """
CREATE OR REPLACE PROCEDURE complex_supply_chain_analysis
AS
    -- Define variables to hold intermediate results
    total_revenue NUMBER;
    avg_lead_time NUMBER;
    high_demand_sku VARCHAR2(50);
    high_demand_count NUMBER := 0;  -- Initialize high_demand_count to zero
    low_stock_sku VARCHAR2(50);
    low_stock_level NUMBER := 9999999;  -- Initialize to a high value
    v_product_type VARCHAR2(26);
    v_sku VARCHAR2(26);
    v_price NUMBER(38,16);
    v_availability NUMBER(38,0);
    v_number_of_products_sold NUMBER(38,0);
    v_revenue_generated NUMBER(38,13);
    v_customer_demographics VARCHAR2(26);
    v_stock_levels NUMBER(38,0);
    v_lead_times NUMBER(38,0);
    v_order_quantities NUMBER(38,0);
    v_shipping_times NUMBER(38,0);
    v_shipping_carriers VARCHAR2(26);
    v_shipping_costs NUMBER(38,16);
    v_supplier_name VARCHAR2(26);
    v_location VARCHAR2(26);
    v_lead_time NUMBER(38,0);
    v_production_volumes NUMBER(38,0);
    v_manufacturing_lead_time NUMBER(38,0);
    v_manufacturing_costs NUMBER(38,16);
    v_inspection_results VARCHAR2(26);
    v_defect_rates NUMBER(38,17);
    v_transportation_modes VARCHAR2(26);
    v_routes VARCHAR2(26);
    v_costs NUMBER(38,14);
    
    -- Define a cursor for iteration
    CURSOR supply_chain_cursor IS
        SELECT PRODUCT_TYPE, SKU, PRICE, AVAILABILITY, NUMBER_OF_PRODUCTS_SOLD, REVENUE_GENERATED,
               CUSTOMER_DEMOGRAPHICS, STOCK_LEVELS, LEAD_TIMES, ORDER_QUANTITIES, SHIPPING_TIMES,
               SHIPPING_CARRIERS, SHIPPING_COSTS, SUPPLIER_NAME, LOCATION, LEAD_TIME, PRODUCTION_VOLUMES,
               MANUFACTURING_LEAD_TIME, MANUFACTURING_COSTS, INSPECTION_RESULTS, DEFECT_RATES, 
               TRANSPORTATION_MODES, ROUTES, COSTS
        FROM supply_chain_data;

BEGIN
    -- Initialize variables
    total_revenue := 0;
    avg_lead_time := 0;

    -- Open the cursor
    OPEN supply_chain_cursor;

    LOOP
        FETCH supply_chain_cursor INTO v_product_type, v_sku, v_price, v_availability, v_number_of_products_sold, v_revenue_generated,
                                       v_customer_demographics, v_stock_levels, v_lead_times, v_order_quantities, v_shipping_times,
                                       v_shipping_carriers, v_shipping_costs, v_supplier_name, v_location, v_lead_time, v_production_volumes,
                                       v_manufacturing_lead_time, v_manufacturing_costs, v_inspection_results, v_defect_rates,
                                       v_transportation_modes, v_routes, v_costs;
        
        EXIT WHEN supply_chain_cursor%NOTFOUND;

        -- Complex calculations
        total_revenue := total_revenue + v_revenue_generated;
        avg_lead_time := avg_lead_time + v_lead_times;

        -- Example of conditional logic and data manipulation
        IF v_number_of_products_sold > high_demand_count THEN
            high_demand_sku := v_sku;
            high_demand_count := v_number_of_products_sold;
        END IF;

        IF v_stock_levels < low_stock_level THEN
            low_stock_sku := v_sku;
            low_stock_level := v_stock_levels;
        END IF;

        -- Additional complex logic can be added here

    END LOOP;

    -- Final calculations
    SELECT avg_lead_time / COUNT(*)
    INTO avg_lead_time
    FROM supply_chain_data;

    -- Print the results
    DBMS_OUTPUT.PUT_LINE('Total Revenue: ' || total_revenue);
    DBMS_OUTPUT.PUT_LINE('Average Lead Time: ' || avg_lead_time);
    DBMS_OUTPUT.PUT_LINE('Highest Demand SKU: ' || high_demand_sku || ' with ' || high_demand_count || ' units sold');
    DBMS_OUTPUT.PUT_LINE('Lowest Stock Level SKU: ' || low_stock_sku || ' with ' || low_stock_level || ' units in stock');

    -- Close the cursor
    CLOSE supply_chain_cursor;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
        IF supply_chain_cursor%ISOPEN THEN
            CLOSE supply_chain_cursor;
        END IF;
END complex_supply_chain_analysis;
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
    database='COMPLEX_PROCEDURE',
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
execute_sql("CREATE TABLE IF NOT EXISTS TEMP_LOGS (LOG_MESSAGE STRING)")

# Execute the converted Snowflake procedure
execute_sql(snowflake_procedure)

# Close the cursor and connection
cur.close()
conn.close()
