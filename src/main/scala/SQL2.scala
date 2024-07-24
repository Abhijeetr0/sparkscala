SELECT employees.name, dept_name
FROM employees
  LEFT JOIN departments
ON employees.dept_id = Departments.dept_id;
