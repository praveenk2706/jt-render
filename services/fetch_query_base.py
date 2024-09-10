fetch_v9_individual_query = "SELECT * FROM JOINED"

fetch_v9_query_prefix = """
WITH JOINED AS (SELECT\n
        OwnerID_Mapping_Table.Owner_ID AS Owner_ID,\n
        Property_State_Name,\n
        Property_County_Name,\n
        Property_Zip_Code,\n
        Properties_View.APN AS APN,\n
        Owner_First_Name,\n
        Owner_Last_Name,\n
        Owner_Full_Name,\n
        Owner_Mailing_Name,\n
        Owner_2_First_Name,\n
        Owner_2_Last_Name,\n
        Owner_Type,\n
        Mail_Address_Full,\n
        Lot_Acreage,\n
        Property_City,\n
        Property_State_Name_Short,\n
        Property_Zip_or_City_Filled,\n
        Property_Address_Full,\n
        Property_County_FIPS,\n
        Market_Price,\n
        Properties_View.Date,\n
        Significant_Flood_Zones,\n
        slope_mean,\n
        Tree_coverage,\n
        WELL,\n
        Wetlands_Total,\n
        Zip_Code_Matching\n
    FROM\n
        `property-database-370200.LandVision_Dataset.VW_PropertiesTableWD` Properties_View\n
        LEFT JOIN `property-database-370200.Owner_Dataset.OwnerID_Mapping` OwnerID_Mapping_Table ON UPPER(Properties_View.Mail_Street_Address) = UPPER(OwnerID_Mapping_Table.Mail_Street_Address_STD)\n
        AND UPPER(Properties_View.Mail_City) = UPPER(OwnerID_Mapping_Table.Mail_City_STD)\n
        AND UPPER(Properties_View.Mail_State) = UPPER(OwnerID_Mapping_Table.Mail_State_STD)\n
        AND UPPER(Properties_View.Mail_Zip_Code) = UPPER(OwnerID_Mapping_Table.Mail_Zip_Code_STD)\n
    WHERE\n
        OwnerID_Mapping_Table.Owner_ID IS NOT NULL\n
    GROUP BY\n
        OwnerID_Mapping_Table.Owner_ID,\n
        Property_State_Name,\n
        Property_County_Name,\n
        Property_Zip_Code,\n
        Properties_View.APN,\n
        Owner_First_Name,\n
        Owner_Last_Name,\n
        Owner_Full_Name,\n
        Owner_Mailing_Name,\n
        Owner_2_First_Name,\n
        Owner_2_Last_Name,\n
        Owner_Type,\n
        Mail_Address_Full,\n
        Lot_Acreage,\n
        Property_City,\n
        Property_State_Name_Short,\n
        Property_Zip_or_City_Filled,\n
        Property_Address_Full,\n
        Property_County_FIPS,\n
        Market_Price,\n
        Properties_View.Date,
        Significant_Flood_Zones,\n
        slope_mean,\n
        Tree_coverage,\n
        WELL,\n
        Wetlands_Total,\n
        Zip_Code_Matching)\n
"""

fetch_v9_individual_query_full = """
WITH JOINED AS (SELECT\n
        OwnerID_Mapping_Table.Owner_ID AS Owner_ID,\n
        Property_State_Name,\n
        Property_County_Name,\n
        Property_Zip_Code,\n
        Properties_View.APN AS APN,\n
        Owner_First_Name,\n
        Owner_Last_Name,\n
        Owner_Full_Name,\n
        Owner_Mailing_Name,\n
        Owner_2_First_Name,\n
        Owner_2_Last_Name,\n
        Owner_Type,\n
        Mail_Address_Full,\n
        Lot_Acreage,\n
        Property_City,\n
        Property_State_Name_Short,\n
        Property_Zip_or_City_Filled,\n
        Property_Address_Full,\n
        Property_County_FIPS,\n
        Market_Price,\n
        Properties_View.Date\n,
        Significant_Flood_Zones,\n
        slope_mean,\n
        Tree_coverage,\n
        WELL,\n
        Wetlands_Total,\n
        Zip_Code_Matching\n
    FROM\n
        `property-database-370200.LandVision_Dataset.VW_PropertiesTableWD` Properties_View\n
        LEFT JOIN `property-database-370200.Owner_Dataset.OwnerID_Mapping` OwnerID_Mapping_Table ON UPPER(Properties_View.Mail_Street_Address) = UPPER(OwnerID_Mapping_Table.Mail_Street_Address_STD)\n
        AND UPPER(Properties_View.Mail_City) = UPPER(OwnerID_Mapping_Table.Mail_City_STD)\n
        AND UPPER(Properties_View.Mail_State) = UPPER(OwnerID_Mapping_Table.Mail_State_STD)\n
        AND UPPER(Properties_View.Mail_Zip_Code) = UPPER(OwnerID_Mapping_Table.Mail_Zip_Code_STD)\n
    WHERE\n
        OwnerID_Mapping_Table.Owner_ID IS NOT NULL\n
    GROUP BY\n
        OwnerID_Mapping_Table.Owner_ID,\n
        Property_State_Name,\n
        Property_County_Name,\n
        Property_Zip_Code,\n
        Properties_View.APN,\n
        Owner_First_Name,\n
        Owner_Last_Name,\n
        Owner_Full_Name,\n
        Owner_Mailing_Name,\n
        Owner_2_First_Name,\n
        Owner_2_Last_Name,\n
        Owner_Type,\n
        Mail_Address_Full,\n
        Lot_Acreage,\n
        Property_City,\n
        Property_State_Name_Short,\n
        Property_Zip_or_City_Filled,\n
        Property_Address_Full,\n
        Property_County_FIPS,\
        Market_Price,\n
        Properties_View.Date,
        Significant_Flood_Zones,\n
        slope_mean,\n
        Tree_coverage,\n
        WELL,\n
        Wetlands_Total,\n
        Zip_Code_Matching)\n
SELECT * FROM JOINED
"""


fetch_v10_individual_query_full = """
WITH J0 AS (\n
SELECT\n
    OwnerID_Mapping_Table.Owner_ID AS Owner_ID,\n
    Property_State_Name,\n
    Property_County_Name,\n
    Property_Zip_Code,\n
    Properties_View.APN AS APN,\n
    Owner_First_Name,\n
    Owner_Last_Name,\n
    Owner_Full_Name,\n
    Owner_Mailing_Name,\n
    Owner_2_First_Name,\n
    Owner_2_Last_Name,\n
    Owner_Type,\n
    Mail_Address_Full,\n
    Lot_Acreage,\n
    Property_City,\n
    Property_State_Name_Short,\n
    Property_Zip_or_City_Filled,\n
    Property_Address_Full,\n
    Property_County_FIPS,\n
    Market_Price,\n
    Properties_View.Date,\n
    Significant_Flood_Zones,\n
    slope_mean,\n
    Tree_coverage,\n
    WELL,\n
    Wetlands_Total,\n
    Zip_Code_Matching\n
FROM\n
    `property-database-370200.LandVision_Dataset.VW_PropertiesTableWD` Properties_View\n
    LEFT JOIN `property-database-370200.Owner_Dataset.OwnerID_Mapping` OwnerID_Mapping_Table \n
    ON \n
    UPPER(Properties_View.Mail_Street_Address) = UPPER(OwnerID_Mapping_Table.Mail_Street_Address_STD)\n
    AND UPPER(Properties_View.Mail_City) = UPPER(OwnerID_Mapping_Table.Mail_City_STD)\n
    AND UPPER(Properties_View.Mail_State) = UPPER(OwnerID_Mapping_Table.Mail_State_STD)\n
    AND UPPER(Properties_View.Mail_Zip_Code) = UPPER(OwnerID_Mapping_Table.Mail_Zip_Code_STD)\n
WHERE\n
    OwnerID_Mapping_Table.Owner_ID IS NOT NULL\n
),\n
Joined AS (\n
SELECT \n
    J0.Owner_ID,\n
    J0.Property_State_Name,\n
    J0.Property_County_Name,\n
    J0.Property_Zip_Code,\n
    J0.APN AS APN,\n
    J0.Owner_First_Name,\n
    J0.Owner_Last_Name,\n
    J0.Owner_Full_Name,\n
    J0.Owner_Mailing_Name,\n
    J0.Owner_2_First_Name,\n
    J0.Owner_2_Last_Name,\n
    J0.Owner_Type,\n
    J0.Mail_Address_Full,\n
    J0.Lot_Acreage,\n
    J0.Property_City,\n
    J0.Property_State_Name_Short,\n
    J0.Property_Zip_or_City_Filled,\n
    J0.Property_Address_Full,\n
    J0.Property_County_FIPS,\n
    J0.Market_Price,\n
    J0.Date,\n
    J0.Significant_Flood_Zones,\n
    J0.slope_mean,\n
    J0.Tree_coverage,\n
    J0.WELL,\n
    J0.Wetlands_Total,\n
    J0.Zip_Code_Matching,\n
    Access_Table.Access_Final\n
FROM J0\n
    LEFT JOIN `property-database-370200.Access_Dataset.ALL_Access` as Access_Table\n
ON\n
    UPPER(J0.APN) = UPPER(Access_Table.APN)\n
    AND UPPER(J0.Property_County_Name) = UPPER(Access_Table.Property_County_Name)\n
    AND UPPER(J0.Property_State_Name) = UPPER(Access_Table.Property_State_Name)\n
    WHERE Access_Table.Access_Final IS NOT NULL\n
Group By\n
    J0.Owner_ID,\n
    J0.Property_State_Name,\n
    J0.Property_County_Name,\n
    J0.Property_Zip_Code,\n
    J0.APN,\n
    J0.Owner_First_Name,\n
    J0.Owner_Last_Name,\n
    J0.Owner_Full_Name,\n
    J0.Owner_Mailing_Name,\n
    J0.Owner_2_First_Name,\n
    J0.Owner_2_Last_Name,\n
    J0.Owner_Type,\n
    J0.Mail_Address_Full,\n
    J0.Lot_Acreage,\n
    J0.Property_City,\n
    J0.Property_State_Name_Short,\n
    J0.Property_Zip_or_City_Filled,\n
    J0.Property_Address_Full,\n
    J0.Property_County_FIPS,\n
    J0.Market_Price,\n
    J0.Date,\n
    J0.Significant_Flood_Zones,\n
    J0.slope_mean,\n
    J0.Tree_coverage,\n
    J0.WELL,\n
    J0.Wetlands_Total,\n
    J0.Zip_Code_Matching,\n
    Access_Table.Access_Final\n
)\n
SELECT * FROM JOINED
"""


fetch_v10_prefix = """
WITH J0 AS (
SELECT
    OwnerID_Mapping_Table.Owner_ID AS Owner_ID,
    Property_State_Name,
    Property_County_Name,
    Property_Zip_Code,
    Properties_View.APN AS APN,
    Owner_First_Name,
    Owner_Last_Name,
    Owner_Full_Name,
    Owner_Mailing_Name,
    Owner_2_First_Name,
    Owner_2_Last_Name,
    Owner_Type,
    Mail_Address_Full,
    Lot_Acreage,
    Property_City,
    Property_State_Name_Short,
    Property_Zip_or_City_Filled,
    Property_Address_Full,
    Property_County_FIPS,
    Market_Price,
    Properties_View.Date,
    Significant_Flood_Zones,
    slope_mean,
    Tree_coverage,
    WELL,
    Wetlands_Total,
    Zip_Code_Matching
FROM
    `property-database-370200.LandVision_Dataset.VW_PropertiesTableWD` Properties_View
    LEFT JOIN `property-database-370200.Owner_Dataset.OwnerID_Mapping` OwnerID_Mapping_Table 
    ON 
    UPPER(Properties_View.Mail_Street_Address) = UPPER(OwnerID_Mapping_Table.Mail_Street_Address_STD)
    AND UPPER(Properties_View.Mail_City) = UPPER(OwnerID_Mapping_Table.Mail_City_STD)
    AND UPPER(Properties_View.Mail_State) = UPPER(OwnerID_Mapping_Table.Mail_State_STD)
    AND UPPER(Properties_View.Mail_Zip_Code) = UPPER(OwnerID_Mapping_Table.Mail_Zip_Code_STD)
WHERE
    OwnerID_Mapping_Table.Owner_ID IS NOT NULL
),
Joined AS (
SELECT 
    J0.Owner_ID,
    J0.Property_State_Name,
    J0.Property_County_Name,
    J0.Property_Zip_Code,
    J0.APN AS APN,
    J0.Owner_First_Name,
    J0.Owner_Last_Name,
    J0.Owner_Full_Name,
    J0.Owner_Mailing_Name,
    J0.Owner_2_First_Name,
    J0.Owner_2_Last_Name,
    J0.Owner_Type,
    J0.Mail_Address_Full,
    J0.Lot_Acreage,
    J0.Property_City,
    J0.Property_State_Name_Short,
    J0.Property_Zip_or_City_Filled,
    J0.Property_Address_Full,
    J0.Property_County_FIPS,
    J0.Market_Price,
    J0.Date,
    J0.Significant_Flood_Zones,
    J0.slope_mean,
    J0.Tree_coverage,
    J0.WELL,
    J0.Wetlands_Total,
    J0.Zip_Code_Matching,
    Access_Table.Access_Final
FROM J0
    LEFT JOIN `property-database-370200.Access_Dataset.ALL_Access` as Access_Table
ON
    UPPER(J0.APN) = UPPER(Access_Table.APN)
    AND UPPER(J0.Property_County_Name) = UPPER(Access_Table.Property_County_Name)
    AND UPPER(J0.Property_State_Name) = UPPER(Access_Table.Property_State_Name)
    WHERE Access_Table.Access_Final IS NOT NULL
Group By
    J0.Owner_ID,
    J0.Property_State_Name,
    J0.Property_County_Name,
    J0.Property_Zip_Code,
    J0.APN,
    J0.Owner_First_Name,
    J0.Owner_Last_Name,
    J0.Owner_Full_Name,
    J0.Owner_Mailing_Name,
    J0.Owner_2_First_Name,
    J0.Owner_2_Last_Name,
    J0.Owner_Type,
    J0.Mail_Address_Full,
    J0.Lot_Acreage,
    J0.Property_City,
    J0.Property_State_Name_Short,
    J0.Property_Zip_or_City_Filled,
    J0.Property_Address_Full,
    J0.Property_County_FIPS,
    J0.Market_Price,
    J0.Date,
    J0.Significant_Flood_Zones,
    J0.slope_mean,
    J0.Tree_coverage,
    J0.WELL,
    J0.Wetlands_Total,
    J0.Zip_Code_Matching,
    Access_Table.Access_Final
)\n
"""


fetch_v11_prefix = """
WITH J0 AS (
SELECT
    OwnerID_Mapping_Table.Owner_ID AS Owner_ID,
    Property_State_Name,
    Property_County_Name,
    Property_Zip_Code,
    Properties_View.APN AS APN,
    Owner_First_Name,
    Owner_Last_Name,
    Owner_Full_Name,
    Owner_Mailing_Name,
    Owner_2_First_Name,
    Owner_2_Last_Name,
    Owner_Type,
    Mail_Address_Full,
    Lot_Acreage,
    Property_City,
    Property_State_Name_Short,
    Property_Zip_or_City_Filled,
    Property_Address_Full,
    Property_County_FIPS,
    Market_Price,
    Properties_View.Date,
    Significant_Flood_Zones,
    slope_mean,
    Tree_coverage,
    WELL,
    Wetlands_Total,
    Zip_Code_Matching
FROM
    `property-database-370200.LandVision_Dataset.VW_PropertiesTableWD` Properties_View
    LEFT JOIN `property-database-370200.Owner_Dataset.OwnerID_Mapping` OwnerID_Mapping_Table 
    ON 
    UPPER(Properties_View.Mail_Street_Address) = UPPER(OwnerID_Mapping_Table.Mail_Street_Address_STD)
    AND UPPER(Properties_View.Mail_City) = UPPER(OwnerID_Mapping_Table.Mail_City_STD)
    AND UPPER(Properties_View.Mail_State) = UPPER(OwnerID_Mapping_Table.Mail_State_STD)
    AND UPPER(Properties_View.Mail_Zip_Code) = UPPER(OwnerID_Mapping_Table.Mail_Zip_Code_STD)
),
Parent AS (
SELECT 
    J0.Owner_ID,
    J0.Property_State_Name,
    J0.Property_County_Name,
    J0.Property_Zip_Code,
    J0.APN AS APN,
    J0.Owner_First_Name,
    J0.Owner_Last_Name,
    J0.Owner_Full_Name,
    J0.Owner_Mailing_Name,
    J0.Owner_2_First_Name,
    J0.Owner_2_Last_Name,
    J0.Owner_Type,
    J0.Mail_Address_Full,
    J0.Lot_Acreage,
    J0.Property_City,
    J0.Property_Address_Full,
    J0.Market_Price,
    J0.Significant_Flood_Zones,
    J0.slope_mean,
    J0.Tree_coverage,
    J0.WELL,
    J0.Wetlands_Total,
    J0.Zip_Code_Matching,
    Access_Table.Access_Final
FROM J0
    LEFT JOIN `property-database-370200.Access_Dataset.ALL_Access` as Access_Table
ON
    UPPER(J0.APN) = UPPER(Access_Table.APN)
    AND UPPER(J0.Property_County_Name) = UPPER(Access_Table.Property_County_Name)
    AND UPPER(J0.Property_State_Name) = UPPER(Access_Table.Property_State_Name)
),
Joined AS (
  SELECT *, CASE WHEN Access_Final is NULL then 'No Information' ELSE Access_Final END As Access_Type FROM Parent
    WHERE Parent.Owner_ID is not NULL and Parent.Property_State_Name is not null and Parent.property_county_name is not null and Parent.apn is not null and Parent.lot_acreage is not null and Parent.mail_address_full is not null and Owner_Mailing_Name is not null)\n
"""


fetch_v12_prefix = """
WITH A AS (
  SELECT Properties.*, 
Mailer.Owner_ID AS Owner_ID
FROM 
`property-database-370200.LandVision_Dataset.VW_PropertiesTableWD` Properties
LEFT JOIN `property-database-370200.Mailer_Dataset.VW_Mailers_Data` Mailer
ON LOWER(Properties.APN) = LOWER(Mailer.APN)
WHERE Owner_ID IS NOT NULL
),
B AS (
  SELECT A.*, Access_Table.Access_Final AS Access_Type
  FROM A
  LEFT JOIN `property-database-370200.Access_Dataset.ALL_Access` Access_Table
  ON LOWERA.APN = LOWER(Access_Table.APN)
  WHERE Access_Type IS NOT NULL
),
Joined AS (
    SELECT * FROM B WHERE LOWER(Property_State_Name) in ('georgia', 'arizona'))\n
"""


fetch_v13_prefix = """WITH
  PropertyOwnerDetailsCTE AS (
  SELECT
    properties.*,
    owners.Owner_First_Name,
    owners.Owner_Last_Name,
    owners.Owner_Short_Name,
    owners.Owner_Name_Type,
    owners.Owner_Type,
    owners.Mail_Street_Address,
    owners.Mail_City,
    owners.Mail_State,
    owners.Mail_Zip_Code,
    owners.Mail_Zip_Code_9,
    owners.Mail_Zip_Code_R,
    owners.Mail_ZipCode_STD,
    owners.Do_Not_Mail,
    owners.Owner_ID AS Owners_ID
  FROM
    `property-database-370200.Dev_Dataset_2.Properties` properties
  JOIN
    `property-database-370200.Dev_Dataset_2.Owners_Data` owners
  ON
    LOWER(REGEXP_REPLACE(properties.Owner_ID, r'\s+', '')) = LOWER(REGEXP_REPLACE(owners.Owner_ID, r'\s+', '')))
"""
