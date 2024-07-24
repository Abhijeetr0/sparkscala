SELECT employees.name, dept_name
FROM employees
  RIGHT JOIN departments
  ON employees.dept_id = Departments.dept_id;