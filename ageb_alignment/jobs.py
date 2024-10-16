# pylint: disable=assignment-from-no-return

from dagster import define_asset_job


generate_framework_job = define_asset_job(
    "generate_framework",
    [
        "*metropoli_list",
        "*framework/municipalities/2000",
        "*framework/municipalities/2010",
        "*framework/municipalities/2020",
        "*framework/states/2000",
        "*framework/states/2010",
        "*framework/states/2020",
        "*framework/agebs/1990",
        "*framework/agebs/2000",
        "*framework/agebs/2010",
        "*framework/agebs/2020",
    ],
)


fix_zones_job = define_asset_job(
    "fix_zones",
    [
        "+zone_agebs/shaped/1990",
        "+zone_agebs/shaped/2000",
        "+zone_agebs/shaped/2010",
        "+zone_agebs/shaped/2020",
    ],
)

generate_gcp_2000_job = define_asset_job("generate_gcp_2000", "+gcp/initial/2000")
