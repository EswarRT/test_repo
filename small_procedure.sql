```sql
CREATE OR REPLACE PROCEDURE add_employee (
    p_employee_id FLOAT,
    p_first_name VARCHAR,
    p_last_name VARCHAR,
    p_department_id FLOAT,
    p_salary FLOAT,
    p_hiredate VARCHAR
)
RETURNS VARCHAR
LANGUAGE javascript
AS
$$
try {
    var sql_command = `INSERT INTO employee (employee_id, first_name, last_name, department_id, salary, hiredate) VALUES (${P_EMPLOYEE_ID}, '${P_FIRST_NAME}', '${P_LAST_NAME}', ${P_DEPARTMENT_ID}, ${P_SALARY}, '${P_HIREDATE}')`;
    snowflake.execute({sqlText: sql_command});
    return 'Successfully added employee';
  }
  catch (err) {
    return 'Failed to add employee: ' + err;
  }
$$
;
```
