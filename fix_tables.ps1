$aws = "C:\Program Files\Amazon\AWSCLIV2\aws.exe"
$tables = @("dim_date", "dim_pollutant", "raw_openaq_measurements", "raw_noaa_gfs_forecast", "refined_air_quality", "refined_weather_forecast", "fact_air_quality_weather")

foreach ($t in $tables) {
    # Get current table definition
    $tableJson = & $aws glue get-table --database-name air_quality_weather_db --name $t --region us-east-1 --output json
    $table = $tableJson | ConvertFrom-Json
    
    # Remove table_type from Parameters if present
    if ($table.Table.Parameters.ContainsKey("table_type")) {
        $table.Table.Parameters.Remove("table_type")
    }
    
    # Ensure classification is parquet
    $table.Table.Parameters["classification"] = "parquet"
    $table.Table.Parameters["format"] = "parquet"
    
    # Update table
    $input = @{
        Name = $table.Table.Name
        StorageDescriptor = $table.Table.StorageDescriptor
        TableType = $table.Table.TableType
        Parameters = $table.Table.Parameters
    } | ConvertTo-Json -Depth 10
    
    $inputPath = "D:\AWS Air Project\table_${t}_input.json"
    $input | Out-File -FilePath $inputPath -Encoding utf8
    
    & $aws glue update-table --database-name air_quality_weather_db --table-input "file://$inputPath" --region us-east-1
    Write-Host "Updated $t"
}