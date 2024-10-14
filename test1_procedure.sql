
CREATE OR REPLACE PROCEDURE manage_employee (
    p_action VARCHAR,
    p_employee_id FLOAT,
    p_first_name VARCHAR,
    p_last_name VARCHAR,
    p_department_id FLOAT,
    p_salary FLOAT,
    p_hire_date DATE
)
RETURNS VARCHAR
LANGUAGE javascript
AS
$$
    var v_sql;
    var stmt;
    var result;
    try{

        if (P_ACTION == 'INSERT') {
            /* Validate inputs */
            if (P_FIRST_NAME == null || P_LAST_NAME == null || P_DEPARTMENT_ID == null || P_SALARY == null || P_HIRE_DATE == null) {
                throw new Error('All parameters must be provided for INSERT.'); 
            }
            
            /* Check if employee_id already exists */
            v_sql = `SELECT 1 FROM employee WHERE employee_id = ${P_EMPLOYEE_ID}`;
            stmt = snowflake.createStatement({sqlText: v_sql});
            result = stmt.execute();
            if(result.next()) {
                throw new Error('Employee with this ID already exists.');
            }
            
            
            /* Insert new employee */
            v_sql = `INSERT INTO employee (employee_id, first_name, last_name, department_id, salary, hire_date) VALUES (${P_EMPLOYEE_ID}, '${P_FIRST_NAME}', '${P_LAST_NAME}', ${P_DEPARTMENT_ID}, ${P_SALARY}, '${P_HIRE_DATE.toISOString()}')`;
            stmt = snowflake.createStatement({sqlText: v_sql});
            result = stmt.execute();
        } 
        else if (P_ACTION == 'UPDATE') {
            /* Validate inputs */
            if (P_EMPLOYEE_ID == null) {
              throw new Error('Employee ID must be provided for UPDATE.');
            }
            
            /* Check if employee exists */
            v_sql = `SELECT 1 FROM employee WHERE employee_id = ${P_EMPLOYEE_ID}`;
            stmt = snowflake.createStatement({sqlText: v_sql});
            result = stmt.execute();
            
            if(!result.next()) {
              throw new Error('Employee with this ID does not exist.');
            }
            
            /* Update employee record */
            v_sql = `UPDATE employee SET first_name = '${P_FIRST_NAME}', last_name = '${P_LAST_NAME}', department_id = ${P_DEPARTMENT_ID}, salary = ${P_SALARY}, hire_date = '${P_HIRE_DATE.toISOString()}' WHERE employee_id = ${P_EMPLOYEE_ID}`;
            stmt = snowflake.createStatement({sqlText: v_sql});
            result = stmt.execute();
        } 
        else if (P_ACTION == 'DELETE') {
            /* Validate inputs */
            if (P_EMPLOYEE_ID == null) {
              throw new Error('Employee ID must be provided for DELETE.');
            }
            
            /* Check if employee exists */
            v_sql = `SELECT 1 FROM employee WHERE employee_id = ${P_EMPLOYEE_ID}`;
            stmt = snowflake.createStatement({sqlText: v_sql});
            result = stmt.execute();
            if(!result.next()) {
              throw new Error('Employee with this ID does not exist.');
            }
            
            /* Delete employee record */
            v_sql = `DELETE FROM employee WHERE employee_id = ${P_EMPLOYEE_ID}`;
            stmt = snowflake.createStatement({sqlText: v_sql});
            result = stmt.execute();
        } 
        else if (P_ACTION == 'RETRIEVE') {
            v_sql = `SELECT * FROM employee WHERE employee_id = ${P_EMPLOYEE_ID}`;
            stmt = snowflake.createStatement({sqlText: v_sql});
            result = stmt.execute();
            if(result.next()) {
                return 'Employee ID: ' + result.EMPLOYEE_ID + '\nFirst Name: ' + result.FIRST_NAME + '\nLast Name: ' + result.LAST_NAME + '\nDepartment ID: ' + result.DEPARTMENT_ID + '\nSalary: ' + result.SALARY + '\nHire Date: ' + result.HIRE_DATE; 
            } else {
                return 'No employee found with ID: ' + P_EMPLOYEE_ID;
            }
        } 
        else {
            throw new Error('Invalid action specified.');
        }

    return 'SUCCESS';

  } catch (err) {
        return err;
  }
$$;

CALL manage_employee('INSERT', 16, 'CM', 'eswar', 106, 67000, '2024-08-05'::DATE);
select * from employee;
CALL manage_employee('UPDATE', 15, 'Roman', 'Reigns', 106, 45000, '2024-02-05'::DATE);
select * from employee;
CALL manage_employee('DELETE', 16, null, null, null, null, null);
select * from employee;
