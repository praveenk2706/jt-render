import pandas as pd
import os

def safe_split_columns(df, col, max_chunk_size=50):
    # This function will split a column into chunks of size `max_chunk_size`
    new_cols = []
    try:
        if df[col].dtype == 'object':  # Process only object (string) columns
            max_len = df[col].str.len().max()
            if pd.isna(max_len) or max_len == 0:
                print(f"Skipping column '{col}' as it has no valid data.")
                return df  # Skip empty or invalid columns
            
            # Split the column into parts of max_chunk_size characters
            for i in range(max_len // max_chunk_size + 1):
                df[f'{col}_{i+1}'] = df[col].str[i*max_chunk_size:(i+1)*max_chunk_size]
                new_cols.append(f'{col}_{i+1}')
            
            df = df.drop(columns=[col])  # Drop the original column

            # Reordering to place new columns where the original one was
            col_position = df.columns.get_loc(new_cols[0])  # Get the first new column index
            df = df[[*df.columns[:col_position], *new_cols, *df.columns[col_position+len(new_cols):]]]
        else:
            print(f"Skipping non-string column '{col}'")
    except Exception as e:
        print(f"Error splitting column '{col}': {e}")
    
    return df

def split_columns(input_folder, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Get a list of CSV files in the input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    
    # Specify the columns to split
    columns_to_split = ['APN', 'Lot_Acreage', 'Market_Price', 'Final_Offer_Price']
    
    for file in csv_files:
        file_path = os.path.join(input_folder, file)
        
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
        
        # Process only specified columns
        for col in columns_to_split:
            if col in df.columns:  # Only process if the column exists in the CSV
                df = safe_split_columns(df, col)

        # Save the processed DataFrame to a new CSV file
        output_file_path = os.path.join(output_folder, file)
        try:
            df.to_csv(output_file_path, index=False)
        except Exception as e:
            print(f"Error saving {output_file_path}: {e}")

# Example usage
split_columns('/home/itechnolabs/Downloads/records', 'sample_mail_house')
