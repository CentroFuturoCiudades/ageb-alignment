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

generate_initial_gcp_job = define_asset_job("generate_gcp", ["+gcp/1990", "+gcp/2000"])

pipeline_1_job = define_asset_job(
    "pipeline_1",
    [
        "zone_agebs/initial/1990",
        "zone_agebs/initial/2000",
        "zone_agebs/initial/2010",
        "zone_agebs/initial/2020",
    ],
)
pipeline_2_job = define_asset_job(
    "pipeline_2",
    [
        "zone_agebs/switched/1990*",
        "zone_agebs/replaced/2000*",
        "zone_agebs/shaped/2010*",
        "zone_agebs/shaped/2020*",
    ],
)
