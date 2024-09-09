import pandas as pd

pricing_records = pd.ExcelFile("./Pricing_Research_1.xlsx")

arizona_sheet = pricing_records.parse(sheet_name="Arizona")

print(arizona_sheet.shape)

properties = [
    {
        "APN": "500-11-0310",
        "Zip": "85193",
        "Lot_Acreage": 320,
        "State": "AZ",
        "County": "PINAL",
    },
    {
        "APN": "510-46-0030",
        "Zip": "85139",
        "Lot_Acreage": 35.17,
        "State": "AZ",
        "County": "PINAL",
    },
    {
        "APN": "501-18-001C",
        "Lot_Acreage": 280.39,
        "County": "PINAL",
        "State": "AZ",
        "Zip": "85139",
    },
    {
        "APN": "501-18-0020",
        "Lot_Acreage": 80.0,
        "County": "PINAL",
        "State": "AZ",
        "Zip": "85139",
    },
    {
        "APN": "503-18-002A",
        "Lot_Acreage": 318.0,
        "County": "PINAL",
        "State": "AZ",
        "Zip": "85172",
    },
    {
        "APN": "R0349033",
        "County": "Coconino",
        "Lot_Acreage": 4.51,
        "State": "AZ",
        "Zip": "86004",
    },
    {
        "APN": "304-34-008",
        "County": "Cochise",
        "Lot_Acreage": 160.00,
        "State": "AZ",
        "Zip": "0",
    },
]


def find_matching_row(
    pricing_data, state, county, zip_code, apn, lot_acreage, **kwargs
):
    pricing_data.sort_values(by=["Per Acre Pricing - Value"])

    state_null = pricing_data["State"].isnull()  # noqa: F841
    county_null = pricing_data["County"].isnull()
    zip_code_null = pricing_data["Zip Code"].isnull()
    acreage_null = (
        pricing_data["Starting Acreage"].isnull()
        & pricing_data["Ending Acreage"].isnull()
    )
    apn_section_null = pricing_data["APN Section"].isnull()

    state_match = pricing_data["State"].str.lower() == str(state).lower()
    county_match = pricing_data["County"].str.lower() == str(county).lower()
    zip_code_match = pricing_data["Zip Code"] == zip_code
    acreage_match = (pricing_data["Starting Acreage"].fillna(0) <= lot_acreage) & (
        pricing_data["Ending Acreage"].fillna(0) >= lot_acreage
    )
    apn_section_match = pricing_data["APN Section"].apply(
        lambda x: str(apn).lower().startswith(str(x).lower().strip())
    )

    results = None
    reason = None

    if not pricing_data[
        state_match & county_match & zip_code_match & apn_section_match & acreage_match
    ].empty:
        results = pricing_data[
            state_match
            & county_match
            & zip_code_match
            & apn_section_match
            & acreage_match
        ]
        reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_match & acreage_match]"

    elif not pricing_data[
        state_match & county_match & zip_code_match & apn_section_match & acreage_null
    ].empty:
        results = pricing_data[
            state_match
            & county_match
            & zip_code_match
            & apn_section_match
            & acreage_null
        ]
        reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_match & acreage_null]"

    elif not pricing_data[
        state_match & county_match & zip_code_match & apn_section_null & acreage_match
    ].empty:
        results = pricing_data[
            state_match
            & county_match
            & zip_code_match
            & apn_section_null
            & acreage_match
        ]
        reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_null & acreage_match]"

    elif not pricing_data[
        state_match & county_match & zip_code_match & apn_section_null & acreage_null
    ].empty:
        results = pricing_data[
            state_match
            & county_match
            & zip_code_match
            & apn_section_null
            & acreage_null
        ]
        reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_null & acreage_null]"

    elif not pricing_data[
        state_match & county_match & zip_code_null & apn_section_match & acreage_match
    ].empty:
        results = pricing_data[
            state_match
            & county_match
            & zip_code_null
            & apn_section_match
            & acreage_match
        ]
        reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_match & acreage_match]"

    elif not pricing_data[
        state_match & county_match & zip_code_null & apn_section_match & acreage_null
    ].empty:
        results = pricing_data[
            state_match
            & county_match
            & zip_code_null
            & apn_section_match
            & acreage_null
        ]
        reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_match & acreage_null]"

    elif not pricing_data[
        state_match & county_match & zip_code_null & apn_section_null & acreage_match
    ].empty:
        results = pricing_data[
            state_match
            & county_match
            & zip_code_null
            & apn_section_null
            & acreage_match
        ]
        reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_null & acreage_match]"

    elif not pricing_data[
        state_match & county_match & zip_code_null & apn_section_null & acreage_null
    ].empty:
        results = pricing_data[
            state_match & county_match & zip_code_null & apn_section_null & acreage_null
        ]
        reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_null & acreage_null]"

    elif not pricing_data[
        state_match & county_null & zip_code_match & apn_section_match & acreage_match
    ].empty:
        results = pricing_data[
            state_match
            & county_null
            & zip_code_match
            & apn_section_match
            & acreage_match
        ]
        reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_match & acreage_match]"

    elif not pricing_data[
        state_match & county_null & zip_code_match & apn_section_match & acreage_null
    ].empty:
        results = pricing_data[
            state_match
            & county_null
            & zip_code_match
            & apn_section_match
            & acreage_null
        ]
        reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_match & acreage_null]"

    elif not pricing_data[
        state_match & county_null & zip_code_match & apn_section_null & acreage_match
    ].empty:
        results = pricing_data[
            state_match
            & county_null
            & zip_code_match
            & apn_section_null
            & acreage_match
        ]
        reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_null & acreage_match]"

    elif not pricing_data[
        state_match & county_null & zip_code_match & apn_section_null & acreage_null
    ].empty:
        results = pricing_data[
            state_match & county_null & zip_code_match & apn_section_null & acreage_null
        ]
        reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_null & acreage_null]"

    elif not pricing_data[
        state_match & county_null & zip_code_null & apn_section_match & acreage_match
    ].empty:
        results = pricing_data[
            state_match
            & county_null
            & zip_code_null
            & apn_section_match
            & acreage_match
        ]
        reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_match & acreage_match]"

    elif not pricing_data[
        state_match & county_null & zip_code_null & apn_section_match & acreage_null
    ].empty:
        results = pricing_data[
            state_match & county_null & zip_code_null & apn_section_match & acreage_null
        ]
        reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_match & acreage_null]"

    elif not pricing_data[
        state_match & county_null & zip_code_null & apn_section_null & acreage_match
    ].empty:
        results = pricing_data[
            state_match & county_null & zip_code_null & apn_section_null & acreage_match
        ]
        reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_null & acreage_match]"

    elif not pricing_data[
        state_match & county_null & zip_code_null & apn_section_null & acreage_null
    ].empty:
        results = pricing_data[
            state_match & county_null & zip_code_null & apn_section_null & acreage_null
        ]
        reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_null & acreage_null]"

    else:
        results = pricing_data[state_match & county_match]
        reason = "pricing_data[state_match & county_match] (no other condition matched)"

    last_row = results.iloc[-1]
    last_row_index = results.index[-1]

    print(
        "matched with last row: "
        + str(last_row_index)
        + " and first row "
        + str(results.index[0])
        + " with reason "
        + str(reason)
    )

    # print(last_row)
    # print(results.iloc[0])

    market_price = lot_acreage * last_row["Per Acre Pricing - Value"]

    print("APN ", apn, " Market Price ", market_price, last_row.to_dict())


for row in properties:
    apn = row["APN"]
    state = row["State"]
    zip = row["Zip"]
    county = row["County"]
    lot_acreage = row["Lot_Acreage"]

    find_matching_row(
        pricing_data=arizona_sheet,
        state=state,
        county=county,
        zip_code=zip,
        apn=apn,
        lot_acreage=lot_acreage,
    )
