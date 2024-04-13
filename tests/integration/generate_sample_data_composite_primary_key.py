import random
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()


def generate_unique_lists(length):
    list1 = []
    list2 = []
    combined_values = set()

    while len(list1) < length:
        value1 = random.randint(0, length * 10)
        value2 = random.randint(0, length * 10)
        combined_value = (value1, value2)

        if combined_value not in combined_values:
            list1.append(value1)
            list2.append(value2)
            combined_values.add(combined_value)

    return list1, list2


row_count = 200

list1, list2 = generate_unique_lists(row_count)
data = {"ID_1": list1, "ID_2": list2, "Name": [], "Age": []}

# Generate 100 rows of sample data
for _ in range(row_count):
    name = fake.name()
    age = fake.random_int(min=18, max=80)
    data["Name"].append(name)
    data["Age"].append(age)

# Create a DataFrame
df = pd.DataFrame(data)

# Write the data into a csv file
outdir = Path(Path(__file__).resolve().parent, "data")
outdir.mkdir(parents=True, exist_ok=True)
outfile = Path(outdir, "people_composite_key.csv")
df.to_csv(outfile, index=False)
