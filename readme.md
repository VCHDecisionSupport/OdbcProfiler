# Generic Profiles

1. Connect to ODBC combatible database
1. Create meta dictionary for all tables/views
1. Create meta dictionary for all columns
1. Merge above two dictionary into single dictionary repersenting all data model of database

`DMMD =` Data Model Meta Dictionary:

    {
        "top_level_key":"table_name",
        "type":"object",
        "properties": {
            standard ODBC meta data (SQLTables),
            "columns": {
                "type":"object",
                "properties": {
                    standard ODBC meta data (SQLColumns)
                }
            }
        }
    }

1. asdf