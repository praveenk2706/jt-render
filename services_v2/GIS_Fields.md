### Fields Added from Didar GIS:

These are the columns I added from GIS CSV.

| fullname                               | type   |
| -------------------------------------- | ------ |
| Clay                                   | FLOAT  |
| Clay_loam                              | FLOAT  |
| Coarse_sand                            | FLOAT  |
| Coarse_sandy_loam                      | FLOAT  |
| Fine_sand                              | FLOAT  |
| Fine_sandy_loam                        | FLOAT  |
| Loam                                   | FLOAT  |
| Loamy_coarse_sand                      | FLOAT  |
| Loamy_fine_sand                        | FLOAT  |
| Loamy_sand                             | FLOAT  |
| Sand                                   | FLOAT  |
| Sandy_clay                             | FLOAT  |
| Sandy_clay_loam                        | FLOAT  |
| Sandy_loam                             | FLOAT  |
| Silt_loam                              | FLOAT  |
| Silty_clay                             | FLOAT  |
| Silty_clay_loam                        | FLOAT  |
| Very_fine_sandy_loam                   | FLOAT  |
| A                                      | FLOAT  |
| AE                                     | FLOAT  |
| AH                                     | FLOAT  |
| AREA_NOT_INCLUDED                      | FLOAT  |
| D                                      | FLOAT  |
| VE                                     | FLOAT  |
| X                                      | FLOAT  |
| Significant_Flood_Zones                | FLOAT  |
| slope_min                              | FLOAT  |
| slope_max                              | FLOAT  |
| slope_mean                             | FLOAT  |
| slope_std                              | FLOAT  |
| num_buildings                          | FLOAT  |
| largest_building                       | FLOAT  |
| smallest_building                      | FLOAT  |
| total_building_area                    | FLOAT  |
| nearest_road_type                      | STRING |
| distance_to_nearest_road_from_centroid | FLOAT  |
| road_length                            | FLOAT  |
| trees_percentage                       | FLOAT  |
| built_percentage                       | FLOAT  |
| grass_percentage                       | FLOAT  |
| crops_percentage                       | FLOAT  |
| shrub_and_scrub_percentage             | FLOAT  |
| bare_percentage                        | FLOAT  |
| water_percentage                       | FLOAT  |
| flooded_vegetation_percentage          | FLOAT  |
| snow_and_ice_percentage                | FLOAT  |
| Estuarine_and_Marine_Deepwater         | FLOAT  |
| Estuarine_and_Marine_Wetland           | FLOAT  |
| Freshwater_Emergent_Wetland            | FLOAT  |
| Freshwater_Forested_Shrub_Wetland      | FLOAT  |
| Freshwater_Pond                        | FLOAT  |
| Lake                                   | FLOAT  |
| Riverine                               | FLOAT  |
| parcel_area                            | FLOAT  |
| plot_area_1                            | FLOAT  |
| largest_rect_area                      | FLOAT  |
| percent_rectangle                      | FLOAT  |
| largest_square_area                    | FLOAT  |
| percent_square                         | FLOAT  |
| largest_rect_area_cleaned              | FLOAT  |
| largest_square_area_cleaned            | FLOAT  |
| topography_new                         | STRING |
| water_land                             | STRING |
| flooded_vegetation_land                | STRING |
| snow_and_ice_land                      | STRING |
| trees_land                             | STRING |
| built_land                             | STRING |
| grass_land                             | STRING |
| crops_land                             | STRING |
| shrub_and_scrub_land                   | STRING |
| bare_land                              | STRING |
| Estuarine_and_Marine_Deepwater_Range   | STRING |
| Estuarine_and_Marine_Range             | STRING |
| Freshwater_Emergent_Range              | STRING |
| Freshwater_Forested_Shrub_Range        | STRING |
| Freshwater_Pond_Range                  | STRING |
| Lake_Range                             | STRING |
| Riverine_Range                         | STRING |
| Clay_Range                             | STRING |
| Clay_loam_Range                        | STRING |
| Coarse_sand_Range                      | STRING |
| Coarse_sandy_loam_Range                | STRING |
| Fine_sand_Range                        | STRING |
| Fine_sandy_loam_Range                  | STRING |
| Loam_Range                             | STRING |
| Loamy_coarse_sand_Range                | STRING |
| Loamy_fine_sand_Range                  | STRING |
| Loamy_sand_Range                       | STRING |
| Sand_Range                             | STRING |
| Sandy_clay_Range                       | STRING |
| Sandy_clay_loam_Range                  | STRING |
| Sandy_loam_Range                       | STRING |
| Silt_loam_Range                        | STRING |
| Silty_clay_Range                       | STRING |
| Silty_clay_loam_Range                  | STRING |
| Very_fine_sandy_loam_Range             | STRING |
| Nearest_Road_Distance                  | STRING |
| Road_Length_Range                      | STRING |
| Wetlands_Total_New                     | FLOAT  |

----

### Columns to refer

1. `Significant_Flood_Zones`
2. `slope_mean`
3. `Wetlands_Total_New`: This field is sum of:
   1. Estuarine_and_Marine_Deepwater   
   2. Estuarine_and_Marine_Wetland     
   3. Freshwater_Emergent_Wetland      
   4. Freshwater_Forested_Shrub_Wetland
   5. Freshwater_Pond                  
   6. Lake                            
   7. Riverine 
4. `topography_new`	
5. `water_land`	
6. `flooded_vegetation_land`	
7. `snow_and_ice_land`	
8. `trees_land`	
9. `built_land`	
10. `grass_land`	
11. `crops_land`	
12. `shrub_and_scrub_land`	
13. `bare_land`	
14. `Estuarine and Marine Deepwater Range`	
15. `Estuarine and Marine Range`	
16. `Freshwater Emergent Range`	
17. `Freshwater Forested/Shrub Range`	
18. `Freshwater Pond Range`	
19. `Lake Range`	
20. `Riverine Range`	
21. `Clay Range`	
22. `Clay loam Range`	
23. `Coarse sand Range`
24. `Coarse sandy loam Range`
25. `Fine sand Range`
26. `Fine sandy loam Range`
27. `Loam Range`
28. `Loamy coarse sand Range`
29. `Loamy fine sand Range`
30. `Loamy sand Range`
31. `Sand Range`
32. `Sandy clay Range`
33. `Sandy clay loam Range`
34. `Sandy loam Range`
35. `Silt loam Range`
36. `Silty clay Range`
37. `Silty clay loam Range`
38. `Very fine sandy loam Range`
39. `Nearest_Road_Distance`
40. `Road_Length_Range`

- Column `topography_new` is a categorical text based column based on `slope_mean` I created using following logic:
  - If `slope_mean` is null or empty: 
    - `No Information`
  - `slope_mean` < 3:
    - `flat`
  - 3 <= `slope_mean` < 8:
    - `some slope`
  - 8 <= `slope_mean` < 15:
    - `moderate slope`
  - `slope_mean` >= 15:
    - `extreme slope`
- Columns from `5` and afterwards are custom categorical text based columns I added which has following unique values and they are based on the percentages of columns in their associated column:
  - No Information
  - 0-25
  - 25-50
  - 50-75
  - 75-100

----

### Missing GIS data report
| Column Name                          | Missing Value representation | Percentage |
| ------------------------------------ | ---------------------------- | ---------- |
| Significant_Flood_Zones              | 'NULL'                       | 73.04%     |
| slope_mean                           | 'NULL'                       | 68.91%     |
| Wetlands_Total_New                   | 'NULL'                       | 80.97%     |
| topography_new                       | 'No Information'             | 68.91%     |
| water_land                           | 'No Information'             | 68.91%     |
| flooded_vegetation_land              | 'No Information'             | 68.91%     |
| snow_and_ice_land                    | 'No Information'             | 68.91%     |
| trees_land                           | 'No Information'             | 81.86%     |
| built_land                           | 'No Information'             | 69.03%     |
| grass_land                           | 'No Information'             | 68.94%     |
| crops_land                           | 'No Information'             | 68.92%     |
| shrub_and_scrub_land                 | 'No Information'             | 68.91%     |
| bare_land                            | 'No Information'             | 68.91%     |
| Estuarine_and_Marine_Deepwater_Range | 'No Information'             | 80.97%     |
| Estuarine_and_Marine_Range           | 'No Information'             | 80.97%     |
| Freshwater_Emergent_Range            | 'No Information'             | 80.97%     |
| Freshwater_Forested_Shrub_Range      | 'No Information'             | 81.00%     |
| Freshwater_Pond_Range                | 'No Information'             | 80.97%     |
| Lake_Range                           | 'No Information'             | 80.97%     |
| Riverine_Range                       | 'No Information'             | 80.97%     |
| Clay_Range                           | 'No Information'             | 69.17%     |
| Clay_loam_Range                      | 'No Information'             | 69.37%     |
| Coarse_sand_Range                    | 'No Information'             | 69.08%     |
| Coarse_sandy_loam_Range              | 'No Information'             | 69.07%     |
| Fine_sand_Range                      | 'No Information'             | 69.12%     |
| Fine_sandy_loam_Range                | 'No Information'             | 69.16%     |
| Loam_Range                           | 'No Information'             | 69.38%     |
| Loamy_coarse_sand_Range              | 'No Information'             | 69.07%     |
| Loamy_fine_sand_Range                | 'No Information'             | 69.09%     |
| Loamy_sand_Range                     | 'No Information'             | 69.93%     |
| Sand_Range                           | 'No Information'             | 69.41%     |
| Sandy_clay_Range                     | 'No Information'             | 69.12%     |
| Sandy_clay_loam_Range                | 'No Information'             | 69.48%     |
| Sandy_loam_Range                     | 'No Information'             | 69.78%     |
| Silt_loam_Range                      | 'No Information'             | 69.19%     |
| Silty_clay_Range                     | 'No Information'             | 69.07%     |
| Silty_clay_loam_Range                | 'No Information'             | 69.11%     |
| Very_fine_sandy_loam_Range           | 'No Information'             | 69.08%     |
| Nearest_Road_Distance                | 'NULL'                       | 0.00%      |
| Road_Length_Range                    | 'No Information'             | 90.98%     |