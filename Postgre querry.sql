-- Dimension Tables

CREATE TABLE agency_dim (
    agency_id INT PRIMARY KEY,
    agency_name VARCHAR(255)
);

CREATE TABLE title_dim (
    title_code INT PRIMARY KEY,
    job_title VARCHAR(255)
);

CREATE TABLE employee_dim (
    employee_id INT PRIMARY KEY,
    full_name VARCHAR(255)
);

-- Fact Table

CREATE TABLE nyc_payroll_combined (
    fiscal_year INT,
    payrollnumber INT,
    agency_id INT,
    agency_name VARCHAR(255),
    employee_id INT,
    agencystartdate DATE,
    worklocationborough VARCHAR(255),
    titlecode INT,
    title_des VARCHAR(255),
    june_30_leavestatus VARCHAR(255),
    basesalary NUMERIC,
    paybasis VARCHAR(255),
    regularhours NUMERIC,
	regulargrosspaid Numeric,
    OT_hours NUMERIC,
    total_OT NUMERIC,
    totalotherpay NUMERIC,
    full_name VARCHAR(255),
    FOREIGN KEY (agency_id) REFERENCES agency_dim (agency_id),
    FOREIGN KEY (employee_id) REFERENCES employee_dim (employee_id),
    FOREIGN KEY (titlecode) REFERENCES title_dim (title_code)
);
select * from agency_dim
INSERT INTO agency_dim (agency_id, agency_name)
SELECT agency_id, agency_name
FROM agency


	
drop table title
delete from title_dim
select * from title_dim
select * from title	
INSERT INTO title_dim (title_code,job_title)
SELECT title_code, job_title
FROM title


delete from employee_dim	
select * from employee
select * from employee_dim
INSERT INTO employee_dim (employee_id, full_name)
SELECT employee_id, full_name
FROM employee

select * from nyc_payroll_combined

drop table nyc_payroll_combined

CREATE TABLE financial_resource_allocation AS
SELECT
    fiscal_year,
    agency_id,
	agency_name,
    SUM(base_salary) AS total_salary,
    SUM(total_ot) AS total_overtime,
    SUM(base_salary + total_ot) AS total_budget_allocation
FROM
    nyc_payroll_combined
GROUP BY
    fiscal_year, agency_id, agency_name;
	
drop table financial_resource_allocation 
select * from financial_resource_allocation 

CREATE TABLE salary_overtime_summary AS
SELECT
    agency_id,
    employee_id,
    fiscal_year,
	agency_name,
    SUM(base_salary) AS total_salary,
    SUM(total_Ot) AS total_overtime
FROM
    nyc_payroll_combined
GROUP BY
    agency_id, employee_id, fiscal_year, agency_name;	

total_OT_paid 
drop table salary_overtime_summary
select * from salary_overtime_summary



SELECT nyc_payroll_combined.agency_name, 
       SUM(base_salary) AS total_salary, 
       SUM(total_Ot) AS total_overtime 
FROM nyc_payroll_combined
JOIN agency_dim ON nyc_payroll_combined.agency_id = agency_dim.agency_id
WHERE fiscal_year = 2020
GROUP BY nyc_payroll_combined.agency_name;


SELECT nyc_payroll_combined.title_des, 
       SUM(base_salary) AS total_salary, 
       SUM(total_Ot) AS total_overtime
FROM nyc_payroll_combined
JOIN title_dim ON nyc_payroll_combined.titlecode = title_dim.title_code
WHERE fiscal_year = 2020
GROUP BY nyc_payroll_combined.title_des;


-- Create the public user
CREATE USER public_user WITH PASSWORD 'password';

-- Grant read-only access to the aggregate tables
GRANT SELECT ON financial_resource_allocation TO public_user;
GRANT SELECT ON salary_overtime_summary TO public_user;


