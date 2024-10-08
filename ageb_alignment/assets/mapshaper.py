import json
import subprocess

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


@asset(deps=["filter_census"])
def clean_census(path_resource: PathResource) -> None:
    out_root_path = Path(path_resource.out_path)
    
    filtered_path = out_root_path / "census_filtered"
    
    out_dir = out_root_path / "census_fixed"
    out_dir.mkdir(exist_ok=True, parents=True)

    for dir_path in filtered_path.glob("*"):
        if not dir_path.is_dir():
            continue
        
        out_subdir_path = out_dir / f"{dir_path.stem}"
        out_subdir_path.mkdir(exist_ok=True, parents=True)
        
        for year in (1990, 2000, 2010, 2020):
            in_path = dir_path / f"{year}.geojson"
            out_path = out_subdir_path / f"{year}.geojson"
            subprocess.check_call(["npx", "mapshaper", "-i", f"\"{in_path}\"", "-clean", "-o", f"\"{out_path}\""], shell=True)

            with open(out_path, "r") as f:
                geom = json.load(f)
            
            geom["name"] = str(year)
            geom["crs"] = {
                "type": "name", 
                "properties": {"name": "urn:ogc:def:crs:EPSG::6372"}
            }
            
            with open(out_path, "w") as f:
                json.dump(geom, f)