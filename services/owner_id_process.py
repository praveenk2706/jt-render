import pandas as pd

sorted_df = pd.read_csv("./sort_df.csv")


print("grouping")
# Group the DataFrame by specified columns
grouped_df = sorted_df.groupby(
    [
        "Mail_Street_Address_R",
    ]
)
print("grouped")
# Initialize a counter for the owner ID
owner_id_counter = 1

# Dictionary to store owner IDs for each group
owner_ids_dict = {}
max_num_length = max(8, len(grouped_df))

print(len(grouped_df))

owner_id_counter = 1
owner_ids_dict = {}

# Iterate over groups in chunks
for chunk_id, chunk in sorted_df.groupby(
    [
        "Mail_Street_Address_R",
    ],
    sort=False,
):
    print("processing chunk ", chunk_id, end=" -.-.- ")
    for row_id, row in chunk.iterrows():
        print("processing row ", row_id, end=" --- ")
        group_key = row["Mail_Street_Address_R"]
        if group_key not in owner_ids_dict:
            owner_ids_dict[group_key] = f"OW-{str(owner_id_counter).zfill(8)}"
            owner_id_counter += 1

        print(owner_ids_dict[group_key])
        sorted_df.loc[row.name, "Owner_ID"] = owner_ids_dict[group_key]
    print("processed chunk ", chunk_id)


sorted_df.to_csv("./owner_id_sorted.csv", index=False)
