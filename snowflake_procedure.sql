```sql
CREATE OR REPLACE PROCEDURE add_employee (
    p_employee_id FLOAT,
    p_first_name VARCHAR(100),
    p_last_name VARCHAR(100),
    p_department_id FLOAT,
    p_salary FLOAT,
    p_hiredate VARCHAR(100)
)
RETURNS STRING
LANGUAGE javascript
AS
$$
try {
    var sql_command = `INSERT INTO employee (employee_id, first_name, last_name, department_id, salary, hiredate) VALUES (?, ?, ?, ?, ?, TO_DATE(?, 'YYYY-MM-DD'))`;
    var stmt = snowflake.createStatement({sqlText: sql_command, binds: [P_EMPLOYEE_ID, P_FIRST_NAME, P_LAST_NAME, P_DEPARTMENT_ID, P_SALARY,P_HIREDATE]});
    var res = stmt.execute();
    return 'Procedure executed successfully';
  }
  catch (err) {
    return 'Error: ' + err.message;
  }
$$
;
```