
CREATE OR REPLACE PROCEDURE manage_employee (
    p_action         VARCHAR,
    p_employee_id    FLOAT,
    p_first_name     VARCHAR,
    p_last_name      VARCHAR,
    p_department_id  FLOAT,
    p_salary         FLOAT,
    p_hire_date      DATE
)
RETURNS VARCHAR
LANGUAGE javascript
AS
$$
    var result = "";
    try {
      var connection = snowflake.createStatement({sqlText: "BEGIN; END;"});
       connection.execute(); 
      var sql_stmt = "";
      switch (p_action) {
        case 'INSERT':
          sql_stmt = `INSERT INTO employee (employee_id, first_name, last_name, department_id, salary, hire_date) VALUES (?, ?, ?, ?, ?, ?)`;
          var insert_stmt = snowflake.createStatement({sqlText: sql_stmt, binds: [p_employee_id, p_first_name, p_last_name, p_department_id, p_salary, p_hire_date.toISOString()]});
          var insert_result = insert_stmt.execute();
          result = "Data Inserted Successfully!";
          break;
        case 'UPDATE':
          sql_stmt = `UPDATE employee SET first_name = ?, last_name = ?, department_id = ?, salary = ?, hire_date = ? WHERE employee_id = ?`;
          var update_stmt = snowflake.createStatement({sqlText: sql_stmt, binds: [p_first_name, p_last_name, p_department_id, p_salary, p_hire_date.toISOString(), p_employee_id]});
          var update_result = update_stmt.execute();
          result = "Data Updated Successfully!";
          break;
        case 'DELETE':
          sql_stmt = `DELETE FROM employee WHERE employee_id = ?`;
          var delete_stmt = snowflake.createStatement({sqlText: sql_stmt, binds: [p_employee_id]});
          var delete_result = delete_stmt.execute();
          result = "Data Deleted Successfully!";
          break;
        case 'RETRIEVE':
          sql_stmt = `SELECT employee_id, first_name, last_name, department_id, salary, hire_date FROM employee WHERE employee_id = ?`;
          var select_stmt = snowflake.createStatement({sqlText: sql_stmt, binds: [p_employee_id]});
          var select_result = select_stmt.execute();
          if(select_result.next()) {
              result = `Employee ID: ${select_result.getColumnValue(1)}\nFirst Name: ${select_result.getColumnValue(2)}\nLast Name: ${select_result.getColumnValue(3)}\nDepartment ID: ${select_result.getColumnValue(4)}\nSalary: ${select_result.getColumnValue(5)}\nHire Date: ${select_result.getColumnValue(6)}`;
          } 
          else {
              result = 'No employee found with ID ' + p_employee_id;
          }
          break;
        default:
          result = 'Invalid action specified.';
          break;
      }
      connection.close();
    } 
    catch (err) {
      result = "Error: " + err.message;
    }
    return result;
  $$;


-- Calling the Stored Procedure for different operations.

CALL manage_employee(
        'INSERT',
        17,
        'New',
        'Employee',
        106,
        67000,
        '2024-08-05'
    );
    
CALL manage_employee(
        'UPDATE',
        17,
        'Updated',
        'Employeename',
        106,
        70000,
        '2024-08-09'
    );
    
CALL manage_employee(
        'RETRIEVE',
        17,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    );

CALL manage_employee(
        'DELETE',
        17,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    );
