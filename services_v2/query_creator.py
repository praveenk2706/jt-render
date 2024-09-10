base_query = """
WITH Merged AS (
    SELECT * FROM `project-database-370200.Dev_Dataset_2.Properties_GA` Properties
    JOIN `properties-database-370200.Dev_Dataset_2.Owners_Data`
)
"""