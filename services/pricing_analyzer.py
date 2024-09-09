import numpy as np
import pandas as pd


def load_data(file_path):
    return pd.ExcelFile(file_path, engine="openpyxl")


def code(raw_csv_path, excel_path):
    pricing_excel_file = load_data("./pricing_research.xlsx")

    rows = pd.read_csv("./draw.csv").to_dict(orient="records")
    # Filter pricing_data based on the current row's values

    output_rows = []
    unmatched_rows = []

    try:

        for index, row in enumerate(rows):

            state_tmp = row["Property_State_Name"]

            state = ""

            pricing_data = pd.DataFrame()

            if str(state_tmp).lower() in ["ga", "georgia"]:
                state = "GA"
                pricing_data = pricing_excel_file.parse(sheet_name="Georgia")

            elif str(state_tmp).lower() in ["az", "arizona"]:
                state = "AZ"
                pricing_data = pricing_excel_file.parse(sheet_name="Arizona")

            elif str(state_tmp).lower() in ["nc", "north-carolina", "north carolina", "north_carolina"]:
                state = "NC"
                pricing_data = pricing_excel_file.parse(sheet_name="NC")

            if state != "NC" and state != "":

                county = row["Property_County_Name"]
                zip_code = row["Property_Zip_Code"]
                acreage = row["Lot_Acreage"]

                state_null = pricing_data["State"].isnull()
                county_null = pricing_data["County"].isnull()
                zip_code_null = pricing_data["Zip Code"].isnull()
                acreage_null = pricing_data["Starting Acreage"].isnull() & pricing_data["Ending Acreage"].isnull()
                apn_section_null = pricing_data["APN Section"].isnull()

                state_match = pricing_data["State"].str.lower() == str(state).lower()
                county_match = pricing_data["County"].str.lower() == str(county).lower()
                zip_code_match = pricing_data["Zip Code"] == zip_code
                acreage_match = (pricing_data["Starting Acreage"].fillna(0) <= acreage) & (pricing_data["Ending Acreage"].fillna(0) >= acreage)
                apn_section_match = pricing_data["APN Section"].apply(lambda x: str(row["APN"]).lower().startswith(str(x).lower().strip()))

                results = None
                reason = None

                if not pricing_data[state_match & county_match & zip_code_match & apn_section_match & acreage_match].empty:
                    results = pricing_data[state_match & county_match & zip_code_match & apn_section_match & acreage_match]
                    reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_match & acreage_match]"

                elif not pricing_data[state_match & county_match & zip_code_match & apn_section_match & acreage_null].empty:
                    results = pricing_data[state_match & county_match & zip_code_match & apn_section_match & acreage_null]
                    reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_match & acreage_null]"

                elif not pricing_data[state_match & county_match & zip_code_match & apn_section_null & acreage_match].empty:
                    results = pricing_data[state_match & county_match & zip_code_match & apn_section_null & acreage_match]
                    reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_null & acreage_match]"

                elif not pricing_data[state_match & county_match & zip_code_match & apn_section_null & acreage_null].empty:
                    results = pricing_data[state_match & county_match & zip_code_match & apn_section_null & acreage_null]
                    reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_null & acreage_null]"

                elif not pricing_data[state_match & county_match & zip_code_null & apn_section_match & acreage_match].empty:
                    results = pricing_data[state_match & county_match & zip_code_null & apn_section_match & acreage_match]
                    reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_match & acreage_match]"

                elif not pricing_data[state_match & county_match & zip_code_null & apn_section_match & acreage_null].empty:
                    results = pricing_data[state_match & county_match & zip_code_null & apn_section_match & acreage_null]
                    reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_match & acreage_null]"

                elif not pricing_data[state_match & county_match & zip_code_null & apn_section_null & acreage_match].empty:
                    results = pricing_data[state_match & county_match & zip_code_null & apn_section_null & acreage_match]
                    reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_null & acreage_match]"

                elif not pricing_data[state_match & county_match & zip_code_null & apn_section_null & acreage_null].empty:
                    results = pricing_data[state_match & county_match & zip_code_null & apn_section_null & acreage_null]
                    reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_null & acreage_null]"

                elif not pricing_data[state_match & county_null & zip_code_match & apn_section_match & acreage_match].empty:
                    results = pricing_data[state_match & county_null & zip_code_match & apn_section_match & acreage_match]
                    reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_match & acreage_match]"

                elif not pricing_data[state_match & county_null & zip_code_match & apn_section_match & acreage_null].empty:
                    results = pricing_data[state_match & county_null & zip_code_match & apn_section_match & acreage_null]
                    reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_match & acreage_null]"

                elif not pricing_data[state_match & county_null & zip_code_match & apn_section_null & acreage_match].empty:
                    results = pricing_data[state_match & county_null & zip_code_match & apn_section_null & acreage_match]
                    reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_null & acreage_match]"

                elif not pricing_data[state_match & county_null & zip_code_match & apn_section_null & acreage_null].empty:
                    results = pricing_data[state_match & county_null & zip_code_match & apn_section_null & acreage_null]
                    reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_null & acreage_null]"

                elif not pricing_data[state_match & county_null & zip_code_null & apn_section_match & acreage_match].empty:
                    results = pricing_data[state_match & county_null & zip_code_null & apn_section_match & acreage_match]
                    reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_match & acreage_match]"

                elif not pricing_data[state_match & county_null & zip_code_null & apn_section_match & acreage_null].empty:
                    results = pricing_data[state_match & county_null & zip_code_null & apn_section_match & acreage_null]
                    reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_match & acreage_null]"

                elif not pricing_data[state_match & county_null & zip_code_null & apn_section_null & acreage_match].empty:
                    results = pricing_data[state_match & county_null & zip_code_null & apn_section_null & acreage_match]
                    reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_null & acreage_match]"

                elif not pricing_data[state_match & county_null & zip_code_null & apn_section_null & acreage_null].empty:
                    results = pricing_data[state_match & county_null & zip_code_null & apn_section_null & acreage_null]
                    reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_null & acreage_null]"

                else:
                    results = pricing_data[state_match & county_match]
                    print(f"is finally else results {results.empty}")
                    reason = "pricing_data[state_match & county_match] (no other condition matched)"

                # print(reason)
                # print(results)
                # print(type(results))

                if results is not None and isinstance(results, pd.DataFrame) and results.shape[0] > 0:

                    last_row = results.iloc[-1]
                    last_row_index = results.index[-1]

                    print(f"match reason {reason}")

                    if (
                        last_row is not None
                        and last_row["Per Acre Pricing - Value"] is not None
                        and isinstance(last_row["Per Acre Pricing - Value"], (int, float, np.int_, np.float_))
                        and last_row["Per Acre Pricing - Value"] > 0
                    ):

                        row["Per Acre Pricing - Value"] = (
                            last_row["Per Acre Pricing - Value"]
                            if last_row["Per Acre Pricing - Value"] is not None and isinstance(last_row["Per Acre Pricing - Value"], (int, float, np.int_, np.float_))
                            else 0
                        )

                        row["Market_Price"] = round(row["Lot_Acreage"] * row["Per Acre Pricing - Value"], 2)

                        market_price = row["Market_Price"]

                        if market_price > 0:
                            offer_percentage = 0
                            if market_price < 50000:
                                offer_percentage = 0.4
                            elif market_price < 75000:
                                offer_percentage = 0.45
                            elif market_price < 100000:
                                offer_percentage = 0.5
                            elif market_price < 150000:
                                offer_percentage = 0.525
                            else:
                                offer_percentage = 0.55

                            offer_price = round((market_price * offer_percentage) + market_price, 2)

                            row["Offer_Percent"] = offer_percentage
                            row["Offer Price"] = offer_price

                            output_rows.append(row)
                else:
                    print("cannot do matching for index ", index)
                    print(row)
                    print(f"unmatch reason {reason}")
                    print(results)

                    unmatched_rows.append(row)

        # Convert output data to DataFrame
        output_df = pd.DataFrame(output_rows)

        # Output DataFrame to CSV
        output_df.to_csv("new_output.csv", index=False)

        if len(unmatched_rows) > 0:
            unmatched_df = pd.DataFrame(unmatched_rows)
            unmatched_df.to_csv("./unmatched_rows.csv", index=False)

    except Exception as e:
        print(e)


# # load base csv
# base_df = pd.read_csv("./base.csv")

# # Load the second CSV file into a DataFrame
# second_csv = pd.read_csv("./output.csv")

# # Perform an inner join on 'Owner_ID' and 'APN' columns to identify common rows
# common_rows = pd.merge(base_df, second_csv, on=["APN"], how="inner")

# # Exclude common rows from the random subset
# random_subset_excluded = base_df[~base_df.index.isin(common_rows.index)]

# # Write the updated random subset to a CSV file
# random_subset_excluded.to_csv("random_subset_excluded.csv", index=False)

# print("CSV file created successfully with excluded rows.")

if __name__ == "__main__":
    code("raw.csv", "pricing_research.xlsx")
