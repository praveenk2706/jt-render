import pandas as pd

# Load the original CSV file into a DataFrame
input_csv = 'test_sample_pricing.csv'
df = pd.read_csv(input_csv)

# Create a subset DataFrame with only the first 50 rows
df_subset = df.head(50)

# Save the subset DataFrame to a new CSV file
subset_csv = 'sample_pricing_subset.csv'
df_subset.to_csv(subset_csv, index=False)

print(f"Subset CSV file with 50 rows saved as {subset_csv}")