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
    proc_name_match = re.search(r'PROCEDURE\s+(\w+)\s*\((.*?)\)\s*AS', oracle_procedure, re.DOTALL)
    if not proc_name_match:
        raise ValueError("The provided Oracle procedure does not match the expected format.")
    
    proc_name = proc_name_match.group(1)
    parameters = proc_name_match.group(2).strip()
    
    # Convert parameters
    param_list = []
    param_declarations = []
    param_assignments = []
    if parameters:
        for param in re.split(r',\s*', parameters):
            param = param.strip()
            param_parts = re.split(r'\s+', param)
            if len(param_parts) < 3:
                raise ValueError(f"Parameter definition '{param}' does not match the expected format.")
            param_name = param_parts[0]
            param_type = param_parts[2]
            param_direction = param_parts[1].upper()
            param_type = type_mapping.get(param_type.upper(), param_type)
            param_list.append(f'{param_name} {param_type}')
            param_declarations.append(f'var {param_name} = arguments[0].{param_name};')
            if param_direction == 'OUT':
                param_assignments.append(f'arguments[0].{param_name} = "";')
    snowflake_parameters = ', '.join(param_list)
    
    # Extract the body of the procedure
    body_match = re.search(r'BEGIN(.*)END;', oracle_procedure, re.DOTALL)
    if not body_match:
        raise ValueError("The procedure body does not match the expected format.")
    
    procedure_body = body_match.group(1).strip()
    
    # Replace Oracle SQL functions with Snowflake SQL equivalents
    procedure_body = procedure_body.replace("SYSDATE", "CURRENT_TIMESTAMP")
    procedure_body = procedure_body.replace("COMMIT;", "")  # Snowflake auto-commits
    
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
    var v_existing_count = 0;
    var v_new_price = 0;
    var v_old_stock_quantity = 0;
    var v_new_stock_quantity = 0;
    var v_error_message = '';

    // Helper function to execute SQL commands
    function executeSQL(sql_command) {{
        statement = snowflake.createStatement({{sqlText: sql_command}});
        return statement.execute();
    }}

    // Logging function
    function log_operation(log_msg, product_id) {{
        var log_sql = `INSERT INTO product_log (product_id, log_message, log_time)
                       VALUES (` + product_id + `, '` + log_msg + `', CURRENT_TIMESTAMP)`;
        executeSQL(log_sql);
    }}

    // Initialize output parameter
    arguments[0].p_log_message = '';

    // Handle different actions
    if (arguments[0].p_action == 'INSERT') {{
        // Check if product already exists
        var check_sql = `SELECT COUNT(*) AS count FROM products WHERE product_id = ` + arguments[0].p_product_id;
        var rs = executeSQL(check_sql);
        rs.next();
        v_existing_count = rs.getColumnValue("COUNT");
        
        if (v_existing_count > 0) {{
            arguments[0].p_log_message = 'Product already exists with ID ' + arguments[0].p_product_id;
            throw new Error(arguments[0].p_log_message);
        }} else {{
            // Insert new product
            var insert_sql = `INSERT INTO products (product_id, product_name, category, price, stock_quantity)
                              VALUES (` + arguments[0].p_product_id + `, '` + arguments[0].p_product_name + `', '` + arguments[0].p_category + `', ` + arguments[0].p_price + `, ` + arguments[0].p_stock_quantity + `)`;
            executeSQL(insert_sql);
            arguments[0].p_log_message = 'Product inserted successfully with ID ' + arguments[0].p_product_id;
            log_operation(arguments[0].p_log_message, arguments[0].p_product_id);
        }}
    }} else if (arguments[0].p_action == 'UPDATE') {{
        // Update product details
        try {{
            var select_sql = `SELECT price, stock_quantity FROM products WHERE product_id = ` + arguments[0].p_product_id;
            var rs = executeSQL(select_sql);
            rs.next();
            v_new_price = rs.getColumnValue("PRICE");
            v_old_stock_quantity = rs.getColumnValue("STOCK_QUANTITY");
            
            // Calculate new stock quantity
            v_new_stock_quantity = arguments[0].p_stock_quantity - v_old_stock_quantity;
            
            // Update product
            var update_sql = `UPDATE products SET product_name = '` + arguments[0].p_product_name + `', category = '` + arguments[0].p_category + `', price = ` + arguments[0].p_price + `, stock_quantity = ` + v_new_stock_quantity + ` WHERE product_id = ` + arguments[0].p_product_id;
            executeSQL(update_sql);
            
            arguments[0].p_log_message = 'Product updated successfully with ID ' + arguments[0].p_product_id;
            log_operation(arguments[0].p_log_message, arguments[0].p_product_id);
        }} catch (err) {{
            arguments[0].p_log_message = 'Product not found with ID ' + arguments[0].p_product_id;
            throw new Error(arguments[0].p_log_message);
        }}
    }} else if (arguments[0].p_action == 'DELETE') {{
        // Delete product
        var delete_sql = `DELETE FROM products WHERE product_id = ` + arguments[0].p_product_id;
        var rs = executeSQL(delete_sql);
        
        if (rs.getRowCount() == 0) {{
            arguments[0].p_log_message = 'Product not found with ID ' + arguments[0].p_product_id;
            throw new Error(arguments[0].p_log_message);
        }} else {{
            arguments[0].p_log_message = 'Product deleted successfully with ID ' + arguments[0].p_product_id;
            log_operation(arguments[0].p_log_message, arguments[0].p_product_id);
        }}
    }} else if (arguments[0].p_action == 'SELECT') {{
        // Select product details
        var select_sql = `SELECT product_id, product_name, category, price, stock_quantity FROM products WHERE product_id = ` + arguments[0].p_product_id;
        var rs = executeSQL(select_sql);
        while (rs.next()) {{
            arguments[0].p_log_message = 'Product Details - ID: ' + rs.getColumnValue("PRODUCT_ID") + ', Name: ' + rs.getColumnValue("PRODUCT_NAME") + ', Category: ' + rs.getColumnValue("CATEGORY") + ', Price: ' + rs.getColumnValue("PRICE") + ', Stock: ' + rs.getColumnValue("STOCK_QUANTITY");
        }}
        
        if (arguments[0].p_log_message == '') {{
            arguments[0].p_log_message = 'Product not found with ID ' + arguments[0].p_product_id;
            throw new Error(arguments[0].p_log_message);
        }}
    }} else {{
        arguments[0].p_log_message = 'Invalid action specified.';
        throw new Error(arguments[0].p_log_message);
    }}

    result = arguments[0].p_log_message;
}} catch (err) {{
    result = 'An error occurred: ' + err.message;
    log_operation(result, arguments[0].p_product_id);
}}
return result;
$$;
"""
    return snowflake_procedure

# Example Oracle procedure
oracle_procedure = """
CREATE OR REPLACE PROCEDURE manage_products (
    p_action IN VARCHAR2,
    p_product_id IN NUMBER,
    p_product_name IN VARCHAR2,
    p_category IN VARCHAR2,
    p_price IN NUMBER,
    p_stock_quantity IN NUMBER,
    p_log_message OUT VARCHAR2
) AS
    -- Local variables
    v_existing_count NUMBER;
    v_new_price NUMBER;
    v_old_stock_quantity NUMBER;
    v_new_stock_quantity NUMBER;
    v_error_message VARCHAR2(4000);
    
    -- Cursor for logging
    CURSOR log_cursor IS
        SELECT log_message, log_time
        FROM product_log
        WHERE product_id = p_product_id
        ORDER BY log_time DESC;
        
    -- Logging procedure
    PROCEDURE log_operation (log_msg IN VARCHAR2) IS
    BEGIN
        INSERT INTO product_log (product_id, log_message, log_time)
        VALUES (p_product_id, log_msg, SYSDATE);
    EXCEPTION
        WHEN OTHERS THEN
            v_error_message := 'Error logging operation: ' || SQLERRM;
            RAISE_APPLICATION_ERROR(-20001, v_error_message);
    END log_operation;
    
BEGIN
    -- Begin transaction
    SAVEPOINT before_action;
    
    -- Initialize log message
    p_log_message := NULL;
    
    -- Handle different actions
    IF p_action = 'INSERT' THEN
        -- Check if product already exists
        SELECT COUNT(*)
        INTO v_existing_count
        FROM products
        WHERE product_id = p_product_id;
        
        IF v_existing_count > 0 THEN
            p_log_message := 'Product already exists with ID ' || p_product_id;
            RAISE_APPLICATION_ERROR(-20002, p_log_message);
        ELSE
            -- Insert new product
            INSERT INTO products (product_id, product_name, category, price, stock_quantity)
            VALUES (p_product_id, p_product_name, p_category, p_price, p_stock_quantity);
            p_log_message := 'Product inserted successfully with ID ' || p_product_id;
            log_operation(p_log_message);
        END IF;
        
    ELSIF p_action = 'UPDATE' THEN
        -- Update product details
        BEGIN
            SELECT price, stock_quantity
            INTO v_new_price, v_old_stock_quantity
            FROM products
            WHERE product_id = p_product_id
            FOR UPDATE;
            
            -- Calculate new stock quantity
            v_new_stock_quantity := p_stock_quantity - v_old_stock_quantity;
            
            -- Update product
            UPDATE products
            SET product_name = p_product_name,
                category = p_category,
                price = p_price,
                stock_quantity = v_new_stock_quantity
            WHERE product_id = p_product_id;
            
            p_log_message := 'Product updated successfully with ID ' || p_product_id;
            log_operation(p_log_message);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                p_log_message := 'Product not found with ID ' || p_product_id;
                RAISE_APPLICATION_ERROR(-20003, p_log_message);
        END;
        
    ELSIF p_action = 'DELETE' THEN
        -- Delete product
        DELETE FROM products
        WHERE product_id = p_product_id;
        
        IF SQL%ROWCOUNT = 0 THEN
            p_log_message := 'Product not found with ID ' || p_product_id;
            RAISE_APPLICATION_ERROR(-20004, p_log_message);
        ELSE
            p_log_message := 'Product deleted successfully with ID ' || p_product_id;
            log_operation(p_log_message);
        END IF;
        
    ELSIF p_action = 'SELECT' THEN
        -- Select product details
        FOR rec IN (SELECT product_id, product_name, category, price, stock_quantity
                    FROM products
                    WHERE product_id = p_product_id) LOOP
            p_log_message := 'Product Details - ID: ' || rec.product_id ||
                             ', Name: ' || rec.product_name ||
                             ', Category: ' || rec.category ||
                             ', Price: ' || rec.price ||
                             ', Stock: ' || rec.stock_quantity;
        END LOOP;
        
        IF p_log_message IS NULL THEN
            p_log_message := 'Product not found with ID ' || p_product_id;
            RAISE_APPLICATION_ERROR(-20005, p_log_message);
        END IF;
        
    ELSE
        p_log_message := 'Invalid action specified.';
        RAISE_APPLICATION_ERROR(-20006, p_log_message);
    END IF;
    
    -- Commit transaction
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        -- Rollback transaction on error
        ROLLBACK TO SAVEPOINT before_action;
        v_error_message := 'Error occurred: ' || SQLERRM;
        p_log_message := v_error_message;
        log_operation(v_error_message);
        RAISE;
END manage_products;
/ 
"""

# Convert Oracle procedure to Snowflake procedure
snowflake_procedure = convert_oracle_to_snowflake(oracle_procedure)
print(snowflake_procedure)

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
execute_sql(snowflake_procedure)

# Execute the procedure call with appropriate parameters
execute_sql("CALL manage_employee_data('INSERT', 101, 'New Product', 'Category A', 19.99, 100, '');")

# Close the cursor and connection
cur.close()
conn.close()
