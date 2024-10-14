# pylint: disable=assignment-from-no-return

from dagster import define_asset_job


generate_framework_job = define_asset_job(
    "generate_framework",
    [
        "*metropoli_list",
        "*municipalities_2000",
        "*municipalities_2010",
        "*municipalities_2020",
        "*states_2000",
        "*states_2010",
        "*states_2020",
        "*agebs_1990",
        "*agebs_2000",
        "*agebs_2010",
        "*agebs_2020",
    ],
)


fix_zones_job = define_asset_job(
    "fix_zones",
    [
        "+zone_agebs_fixed_1990",
        "+zone_agebs_fixed_2000",
        "+zone_agebs_fixed_2010",
        "+zone_agebs_fixed_2020",
    ],
)

generate_gcp_2000_job = define_asset_job("generate_gcp_2000", "+initial_gcp_2000")
