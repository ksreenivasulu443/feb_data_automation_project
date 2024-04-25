SELECT
    "Identifier",
    "Surname",
    "given_name",
    "middle_initial",
    "Primary_street_number",
    "primary_street_name",
    "city",
    "state",
    "zipcode",
    "Email",
     phone  ,
    "birthmonth",
    'Y' as "Current_ind",
    "batch_date",
    "create_date",
    "update_date",
    "create_user",
    "update_user"
FROM
    contact_info_bronze cib
union all
SELECT
    "Identifier",
    "Surname",
    "given_name",
    "middle_initial",
    "Primary_street_number_prev",
    "primary_street_name_prev",
    "city_prev",
    "state_prev",
    "zipcode_prev",
    "Email",
     phone  ,
    "birthmonth",
    'N' as "Current_ind",
    "batch_date",
    "create_date",
    "update_date",
    "create_user",
    "update_user"
FROM
    contact_info_bronze