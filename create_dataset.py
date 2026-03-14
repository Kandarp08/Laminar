import csv

# Adjust 'rows' to scale the file size
rows = 1_000 
filename = "output_test.csv"

print(f"Generating {filename}...")

with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Name", "Age", "Score"]) # Header
    writer.writerow(["String", "Int", "Float"]) # Datatypes
    for i in range(rows):
        writer.writerow([f"User_{i}", i % 100, i * 1.5])

print("Test data generated.")
