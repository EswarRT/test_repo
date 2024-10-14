CREATE TABLE employee_performance_summary (
    employee_id NUMBER PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department_id NUMBER,
    department_name VARCHAR(100),
    salary FLOAT,
    rank_in_department NUMBER,
    salary_difference_from_avg FLOAT
);

-- Create Table for Department Insights
CREATE TABLE department_insights (
    department_id NUMBER PRIMARY KEY,
    department_name VARCHAR(100),
    top_performing_employee_id NUMBER,
    top_performing_employee_salary FLOAT,
    average_salary FLOAT,
    salary_stddev FLOAT
);

-- Create Table for Salary Trends
CREATE TABLE salary_trends (
    hire_year NUMBER PRIMARY KEY,
    department_id NUMBER,
    department_name VARCHAR(100),
    average_salary FLOAT
);

-- Stored Procedure to Generate Detailed Reports
CREATE OR REPLACE PROCEDURE generate_advanced_reports()
RETURNS VARCHAR
LANGUAGE javascript
AS
$$
  try{
    var employeePerformanceSummary = `INSERT INTO employee_performance_summary (employee_id, first_name, last_name, department_id, department_name, salary, rank_in_department, salary_difference_from_avg)
    SELECT
        e.employee_id,
        e.first_name,
        e.last_name,
        e.department_id,
        d.department_name,
        e.salary,
        RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS rank_in_department,
        e.salary - AVG(e.salary) OVER (PARTITION BY e.department_id) AS salary_difference_from_avg
    FROM
        employees_new e
        JOIN departments_new d ON e.department_id = d.department_id`;
    snowflake.execute({sqlText: employeePerformanceSummary});

    var departmentInsights = `INSERT INTO department_insights (department_id, department_name, top_performing_employee_id, top_performing_employee_salary, average_salary, salary_stddev)
    SELECT
        d.department_id,
        d.department_name,
        top_employee.employee_id AS top_performing_employee_id,
        top_employee.salary AS top_performing_employee_salary,
        AVG(e.salary) AS average_salary,
        STDDEV(e.salary) AS salary_stddev
    FROM
        departments_new d
        JOIN employees_new e ON d.department_id = e.department_id
        LEFT JOIN (
            SELECT
                department_id,
                employee_id,
                salary
            FROM
                (SELECT
                    e.department_id,
                    e.employee_id,
                    e.salary,
                    RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS rank_in_department
                 FROM
                    employees_new e
                )
            WHERE
                rank_in_department = 1
        ) top_employee ON d.department_id = top_employee.department_id
    GROUP BY
        d.department_id, d.department_name, top_employee.employee_id, top_employee.salary`;
    snowflake.execute({sqlText: departmentInsights});

    var salaryTrends = `INSERT INTO salary_trends (hire_year, department_id, department_name, average_salary)
    SELECT
        EXTRACT(YEAR FROM e.hire_date) AS hire_year,
        e.department_id,
        d.department_name,
        AVG(e.salary) AS average_salary
    FROM
        employees_new e
        JOIN departments_new d ON e.department_id = d.department_id
    GROUP BY
        EXTRACT(YEAR FROM e.hire_date), e.department_id, d.department_name`;
    snowflake.execute({sqlText: salaryTrends});
    return 'Successfully Inserted the data into the tables!';
  }
  catch(err){
    return 'Error while inserting the data in tables!' + err.message;
  }
$$
;
-- Execute the Procedure
CALL generate_advanced_reports();
-- View the Results
SELECT * FROM employee_performance_summary;

SELECT * FROM department_insights;

SELECT * FROM salary_trends;


