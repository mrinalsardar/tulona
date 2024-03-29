import pandas as pd
import random
from faker import Faker
from pathlib import Path

# Initialize Faker to generate fake data
fake = Faker()

def generate_employee_data(num_employees):
    # Generate sample data
    data = {
        'Employee_ID': [i+1 for i in range(num_employees)],
        'Name': [fake.name() for _ in range(num_employees)],
        'Email': [fake.email() for _ in range(num_employees)],
        'Address': [fake.address().replace('\n', ', ') for _ in range(num_employees)],
        'Phone_Number': [fake.phone_number() for _ in range(num_employees)],
        'Age': [random.randint(20, 65) for _ in range(num_employees)],
        'Department': [fake.job() for _ in range(num_employees)],
        'Salary': [random.randint(30000, 100000) for _ in range(num_employees)]
    }

    # Create DataFrame
    df = pd.DataFrame(data)
    return df

# Generate 10 sample employee records
sample_employee_data = generate_employee_data(100)

# Write the data into a csv file
outdir = Path(Path(__file__).resolve().parent, 'data')
outdir.mkdir(parents=True, exist_ok=True)
outfile = Path(outdir, 'employee.csv')
sample_employee_data.to_csv(outfile, index=False)