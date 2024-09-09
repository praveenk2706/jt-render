Pricing Matching Approach.


```python
if state_match:
    if county_match:
        if zip_code_match:
            if acreage_match:
                if state match and county match and zip_code_match and acreage_match and apn_match:
                    # last row of filtered data
                    # last index of filtered data
                    # reason: all 5 match
                else:
                    # since apn did not match.
                    #  go back to all rows where acreage matched. 
                    #  filter all rows from them where apn is null.
                    #  if rows found, return last row
                    if state match and county match and zip_code_match and apn_null_match and acreage_match:
                        # last row of this filtered data
                        # reason apn null with acreage match
                    else:
                        #  no rows found which have apn_null and match acreage.
                        #  go back to rows with zip code match.
                        #  get all rows with state, county and zip code match but with empty apn and empty lot acreage.
                        if state match and county match and zip_code_match and apn null and acreage null:
                            # last row of this filtered data
                            # reason apn null with null acreage
                        else:
                            # no rows found with zip code match which satisfy (apn_null_match and acreage_match) or (apn_null_match and acreage_null)
                            # go back to rows where state match and county match and zip code is null and apn is null and acreage is null.
                            # get all such rows.
                            if state match and county match and zip_code is null and apn is null and acreage is null:
                                # last row from these rows
                                # reason state match and county match and zip code is null and apn is null and acreage is null
                            else:
                                # get all rows where state match and county is null
                                if state match and county is null:
                                    # last row from these rows.
                                    # reason: state match and county is null
            else:
                # acreage match not found.
                # get all rows where state match and county match and zip_code_match and apn match and acreage is null
                if state match and county match and zip_code_match and apn match and acreage is null:
                    # last row of filtered data
                    # reason: state match and county match and zip_code_match and apn_match and acreage is null
                else:
                    # get all rows where state match and county match and zip_code_match and apn is null and acreage is null
                    if state match and county match and zip_code_match and apn is null and acreage is null:
                        # last row of filtered data.
                        # reason: state match and county match and zip_code_match and apn is null and acreage is null
                    else:
        else:
            pass
    else:
        pass

else:
    # get all rows where state is null

```