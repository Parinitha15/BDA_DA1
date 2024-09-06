# MapReduce Net Pay Calculation

This project is designed to calculate the net pay of employees from multiple departments based on their basic pay and additional percentages (allowance and tax) from two input files. The program uses MapReduce to process the data and displays the count of employees in each department who earn a net pay greater than 5000 rupees.

## Files
- **Emp.txt**: Contains the employee details in the following format:
  - `emp_id, emp_name, dept_name, basic_pay`
  - Example: `101,John,SCOPE,4000`

- **Pay.txt**: Contains the pay structure for basic pay, including the allowance and tax percentages:
  - `basic_pay, allowance_percentage, tax_percentage`
  - Example: `4000,20,10`

## Net Pay Calculation
The net pay of each employee is calculated using the formula:
- `Net Pay = Basic Pay + Allowance - Tax`
  - Allowance = `(allowance percentage / 100) * Basic Pay`
  - Tax = `(tax percentage / 100) * Basic Pay`

The program displays the number of employees in each department who earn a net pay greater than 5000 rupees.

## MapReduce Flow
1. **Mapper**: 
   - Reads the `Emp.txt` file and associates it with the corresponding allowance and tax percentage from `Pay.txt`.
   - Calculates the net pay for each employee.
   - Outputs the department name and the calculated net pay as the key-value pair.
   
2. **Partitioner**:
   - Uses a custom partitioner to divide the records based on department:
     - `SCOPE` goes to reducer 0.
     - `SENSE` goes to reducer 1.
     - `SELECT` goes to reducer 2.

3. **Reducer**:
   - Receives the key-value pairs for each department.
   - Counts the number of employees in each department with a net pay greater than 5000 rupees.
   - Outputs the department name and the count of employees meeting the criteria.

## Setup and Execution

### Prerequisites
- Java installed (1.7+)
- Hadoop installed and configured properly
- The input files `Emp.txt` and `Pay.txt`

### Compiling the Program
```bash
$ javac -classpath `hadoop classpath` -d . pari22mis1026.java
$ jar -cvf NetPayCalculation.jar -C . .

## Output

![op1](https://github.com/user-attachments/assets/77805967-725d-4678-86e1-1e5790cdcf09)
![op2](https://github.com/user-attachments/assets/a0486cba-16fe-4525-833b-72efa71bb555)

