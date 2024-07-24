SELECT employees.name, dept_name
FROM employees
  INNER JOIN departments
ON employees.dept_id = Departments.dept_id
WHERE Departments.dept_name = Engineering;