import json
import subprocess

aws = r"C:\Program Files\Amazon\AWSCLIV2\aws.exe"
tables = ["dim_date", "dim_pollutant", "raw_openaq_measurements", "raw_noaa_gfs_forecast", "refined_air_quality", "refined_weather_forecast", "fact_air_quality_weather"]

for t in tables:
    # Get current table
    result = subprocess.run([aws, "glue", "get-table", "--database-name", "air_quality_weather_db", "--name", t, "--region", "us-east-1", "--output", "json"], capture_output=True, text=True)
    table = json.loads(result.stdout)["Table"]
    
    # Fix parameters
    params = table.get("Parameters", {})
    params.pop("table_type", None)  # Remove ICEBERG
    params["classification"] = "parquet"
    params["format"] = "parquet"
    table["Parameters"] = params
    
    # Also fix StorageDescriptor.Parameters
    if "StorageDescriptor" in table and "Parameters" in table["StorageDescriptor"]:
        sd_params = table["StorageDescriptor"]["Parameters"]
        sd_params.pop("table_type", None)
        sd_params["classification"] = "parquet"
        sd_params["format"] = "parquet"
    
    # Prepare update input
    update_input = {
        "Name": table["Name"],
        "StorageDescriptor": table["StorageDescriptor"],
        "TableType": table["TableType"],
        "Parameters": table["Parameters"]
    }
    
    # Write to temp file
    with open(f"D:/AWS Air Project/table_{t}_input.json", "w") as f:
        json.dump(update_input, f)
    
    # Update table
    subprocess.run([aws, "glue", "update-table", "--database-name", "air_quality_weather_db", "--table-input", f"file://D:/AWS Air Project/table_{t}_input.json", "--region", "us-east-1"])
    print(f"Updated {t}")

print("All tables updated!")