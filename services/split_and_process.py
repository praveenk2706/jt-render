import random
import traceback

import numpy as np
import pandas as pd


class Dummy:

    def __init__(self) -> None:
        pass

    def process_dataframe(self, df):
        try:
            # Step 1: Filter the DataFrame
            df = df[df["Property_State_Name"].str.lower().isin(["arizona", "georgia"])]

            # Step 2: Group the Data (case-insensitive)
            df["Property_State_Name"] = df["Property_State_Name"].str.title()
            df["Property_County_Name"] = df["Property_County_Name"].str.title()
            grouped = df.groupby(
                ["Owner_ID", "Property_State_Name", "Property_County_Name"],
                as_index=False,
            )

            # Step 3: Shuffle the Groups
            groups = list(grouped)
            np.random.shuffle(groups)

            # Step 4: Divide into State Batches
            arizona_groups = [g for g in groups if g[0][1] == "Arizona"]
            georgia_groups = [g for g in groups if g[0][1] == "Georgia"]

            # Step 5: Split into National and Local Brands
            national_arizona, local_arizona = (
                arizona_groups[: len(arizona_groups) // 2],
                arizona_groups[len(arizona_groups) // 2 :],
            )
            national_georgia, local_georgia = (
                georgia_groups[: len(georgia_groups) // 2],
                georgia_groups[len(georgia_groups) // 2 :],
            )

            # Step 6: Split into Contract Front and Contract Back
            def split_contract(groups):
                half = len(groups) // 2
                return groups[:half], groups[half:]

            national_arizona_front, national_arizona_back = split_contract(
                national_arizona
            )
            local_arizona_front, local_arizona_back = split_contract(local_arizona)
            national_georgia_front, national_georgia_back = split_contract(
                national_georgia
            )
            local_georgia_front, local_georgia_back = split_contract(local_georgia)

            # Step 7: Assign Control Numbers
            def assign_control_numbers(batch):
                population_size = 999999
                batch_size = len(batch)
                sample_size = min(population_size, batch_size)
                control_numbers = random.sample(
                    range(1, max(population_size, batch_size)), sample_size
                )
                return control_numbers

            # Define Mail Group Batch info
            mail_group_info = {
                "national_arizona_front": {
                    "name": "AZ017",
                    "type": "SLI & Contract Front",
                    "phone": "(520) 353-1257",
                },
                "national_arizona_back": {
                    "name": "AZ018",
                    "type": "SLI & Contract Back",
                    "phone": "(520) 337-9208",
                },
                "local_arizona_front": {
                    "name": "AZ015",
                    "type": "AZLF & Contract Front",
                    "phone": "(520) 597-7886",
                },
                "local_arizona_back": {
                    "name": "AZ016",
                    "type": "AZLF & Contract Back",
                    "phone": "(520) 277-0460",
                },
                "national_georgia_front": {
                    "name": "GA026",
                    "type": "SLI & Contract Front",
                    "phone": "(912) 513-4454",
                },
                "national_georgia_back": {
                    "name": "GA027",
                    "type": "SLI & Contract Back",
                    "phone": "(678) 944-7634",
                },
                "local_georgia_front": {
                    "name": "GA024",
                    "type": "GLI & Contract Front",
                    "phone": "(912) 455-7202",
                },
                "local_georgia_back": {
                    "name": "GA025",
                    "type": "GLI & Contract Back",
                    "phone": "(912) 900-1389",
                },
            }

            # Step 8: Update Batches with Additional Info and Control Numbers
            def update_batch(batch, info):
                control_numbers = assign_control_numbers(batch)
                updated_batch = []
                for (key, df), control_number in zip(batch, control_numbers):
                    df["brand_name"] = info["brand_name"]
                    df["website"] = info["website"]
                    df["email"] = info["email"]
                    df["phone"] = info["phone"]
                    df["mail_group_name"] = info["name"]
                    df["type"] = info["type"]
                    df["control_number"] = control_number
                    df["Property_State_Name"] = df["Property_State_Name"].str.title()
                    df["Property_County_Name"] = df["Property_County_Name"].str.title()
                    updated_batch.append(df)
                return updated_batch

            # Assuming the conditions to check if a group is National or Local, these are placeholders
            brand_details = {
                "national": {
                    "brand_name": "Sunset Land Investors",
                    "website": "SunsetLandInvestors.com",
                    "email": "Contact@SunsetLandInvestors.com",
                },
                "local_georgia": {
                    "brand_name": "Georgia Land Investors",
                    "website": "www.GeorgiaLandInvestors.com",
                    "email": "Contact@GeorgiaLandInvestors.com",
                },
                "local_arizona": {
                    "brand_name": "Arizona Land & Farm",
                    "website": "www.ArizonaLandandFarm",
                    "email": "Contact@ArizonaLandandFarm.com",
                },
            }

            batches = []
            for key, groups in zip(
                [
                    "national_arizona_front",
                    "national_arizona_back",
                    "local_arizona_front",
                    "local_arizona_back",
                    "national_georgia_front",
                    "national_georgia_back",
                    "local_georgia_front",
                    "local_georgia_back",
                ],
                [
                    national_arizona_front,
                    national_arizona_back,
                    local_arizona_front,
                    local_arizona_back,
                    national_georgia_front,
                    national_georgia_back,
                    local_georgia_front,
                    local_georgia_back,
                ],
            ):
                if "national" in key:
                    info = {**mail_group_info[key], **brand_details["national"]}
                elif "georgia" in key:
                    info = {**mail_group_info[key], **brand_details["local_georgia"]}
                else:
                    info = {**mail_group_info[key], **brand_details["local_arizona"]}
                batches.extend(update_batch(groups, info))

            # Combine all DataFrames in batches
            combined_df = pd.concat(batches)

            return combined_df

        except Exception as e:
            print(e)
            traceback.print_exc()
            return None


# Example usage:
df = pd.read_csv("./raw.csv")
obj = Dummy()

combined_df = obj.process_dataframe(df)
# Step 9: Export each group to a separate CSV file
file_names = []
for mailer_group, group_df in combined_df.groupby("mail_group_name"):
    file_name = f"{mailer_group}.csv"
    group_df.to_csv(file_name, index=False)
    file_names.append(file_name)
    print(f"Exported DataFrame to {file_name}")

print("Exported CSV files:", file_names)
