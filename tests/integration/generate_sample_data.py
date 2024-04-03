import random
from pathlib import Path
import numpy as np
from datetime import datetime

import pandas as pd
from faker import Faker

# Initialize Faker to generate fake data
fake = Faker()


def generate_employee_data(num_employees):
    # Generate random dates of birth within a reasonable range
    min_dob = datetime(1950, 1, 1)
    max_dob = datetime(2000, 12, 31)

    # Generate random employment start dates within a range
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2020, 12, 31)

    # Generate sample data
    data = {
        "Employee_ID": [i + 1 for i in range(num_employees)],
        "Name": [fake.name() for _ in range(num_employees)],
        "Email": [fake.email() for _ in range(num_employees)],
        "Address": [fake.address().replace("\n", ", ") for _ in range(num_employees)],
        "Phone_Number": [fake.phone_number() for _ in range(num_employees)],
        "Age": [random.randint(20, 65) for _ in range(num_employees)],
        "Department": [fake.job() for _ in range(num_employees)],
        "Salary": [random.randint(30000, 100000) for _ in range(num_employees)],
        "Date_of_Birth": [
            np.random.choice(pd.date_range(min_dob, max_dob)) for _ in range(num_employees)
        ],
        "Employment_Date": [
            np.random.choice(pd.date_range(start_date, end_date)) for _ in range(num_employees)
        ],
    }

    # Create DataFrame
    df = pd.DataFrame(data)
    return df


# Generate 10 sample employee records
sample_employee_data = generate_employee_data(100)

# Write the data into a csv file
outdir = Path(Path(__file__).resolve().parent, "data")
outdir.mkdir(parents=True, exist_ok=True)
outfile = Path(outdir, "employee.csv")
sample_employee_data.to_csv(outfile, index=False)
