import sys
import os
import boto3
import json
from datetime import datetime, timedelta

s3 = boto3.client('s3')

BUCKET = os.environ.get('S3_OUTPUT_BUCKET', 'air-quality-weather-lakehouse-785248360353')
OUTPUT_PREFIX = os.environ.get('S3_OUTPUT_PREFIX', 'raw/noaa_gfs/')

NOAA_BUCKET = 'noaa-gfs-bdp-pds'

CYCLE_HOURS = [0, 6, 12, 18]

# Surface/near-surface variables only for air quality correlation
# Each variable = 1 message with ~1M grid points
# 8 variables * 1M = ~8M records = ~1-2GB JSONL
SURFACE_MESSAGES = {
    ('2t', 'heightAboveGround', 2): 'temp_2m',
    ('2d', 'heightAboveGround', 2): 'dewpoint_2m',
    ('10u', 'heightAboveGround', 10): 'wind_u_10m',
    ('10v', 'heightAboveGround', 10): 'wind_v_10m',
    ('msl', 'meanSea', 0): 'pressure_msl',
    ('tp', 'surface', 0): 'precip_total',
    ('sst', 'surface', 0): 'sst',
    ('sp', 'surface', 0): 'pressure_surface',
}


def find_available_cycle(now):
    base_date = now - timedelta(days=1)
    for days_back in range(0, 3):
        check_date = base_date - timedelta(days=days_back)
        cycle_date = check_date.strftime('%Y%m%d')
        for hour in reversed(CYCLE_HOURS):
            cycle_hour = str(hour).zfill(2)
            prefix = f'gfs.{cycle_date}/{cycle_hour}/atmos/gfs.t{cycle_hour}z.pgrb2.0p25.f000'
            try:
                s3.head_object(Bucket=NOAA_BUCKET, Key=prefix)
                print(f'Found available cycle: s3://{NOAA_BUCKET}/{prefix}')
                return cycle_date, cycle_hour, prefix
            except Exception:
                continue
    return None, None, None


def handler(event, context):
    try:
        now = datetime.utcnow()
        cycle_date, cycle_hour, prefix = find_available_cycle(now)

        if cycle_date is None:
            msg = 'No available NOAA GFS cycle found in the last 3 days'
            print(msg)
            return {'statusCode': 404, 'body': json.dumps({'error': msg})}

        local_path = f'/tmp/noaa_{cycle_date}_{cycle_hour}.grib2'

        print(f'Downloading s3://{NOAA_BUCKET}/{prefix}')
        s3.download_file(NOAA_BUCKET, prefix, local_path)
        file_size = os.path.getsize(local_path)
        print(f'Downloaded {file_size} bytes')

        record_count = decode_grib(local_path, cycle_date, cycle_hour)

        date_formatted = f'{cycle_date[:4]}-{cycle_date[4:6]}-{cycle_date[6:8]}'
        output_key = f'{OUTPUT_PREFIX}cycle_date={date_formatted}/cycle_hour={cycle_hour}/noaa_decode.jsonl'

        s3.upload_file('/tmp/output.jsonl', BUCKET, output_key)
        print(f'Wrote {record_count} records to s3://{BUCKET}/{output_key}')

        for f in [local_path, '/tmp/output.jsonl']:
            try:
                os.remove(f)
            except OSError:
                pass

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Decoded {record_count} records',
                'output_key': output_key
            })
        }
    except Exception as e:
        print(f'Error: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def decode_grib(grib_path, cycle_date, cycle_hour):
    import eccodes
    from datetime import datetime as dt2, timedelta as td

    date_formatted = f'{cycle_date[:4]}-{cycle_date[4:6]}-{cycle_date[6:8]}'
    base_dt = dt2.strptime(f'{cycle_date} {cycle_hour}', '%Y%m%d %H')

    record_count = 0
    skipped_count = 0

    with open(grib_path, 'rb') as gf, open('/tmp/output.jsonl', 'w') as out_f:
        while True:
            gid = eccodes.codes_grib_new_from_file(gf)
            if gid is None:
                break
            try:
                short_name = eccodes.codes_get_string(gid, 'shortName')
                type_of_level = eccodes.codes_get_string(gid, 'typeOfLevel')
                level = eccodes.codes_get(gid, 'level')

                key = (short_name, type_of_level, int(level))
                if key not in SURFACE_MESSAGES:
                    skipped_count += 1
                    continue

                variable_name = SURFACE_MESSAGES[key]
                step = eccodes.codes_get(gid, 'step')

                lats = eccodes.codes_get_array(gid, 'latitudes')
                lons = eccodes.codes_get_array(gid, 'longitudes')
                values = eccodes.codes_get_array(gid, 'values')

                forecast_step_hours = int(step)
                fc_dt = base_dt + td(hours=forecast_step_hours)
                forecast_time = fc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                for i in range(len(values)):
                    record = json.dumps({
                        'variable': variable_name,
                        'value': round(float(values[i]), 4),
                        'latitude': round(float(lats[i]), 4),
                        'longitude': round(float(lons[i]), 4),
                        'forecast_time': forecast_time,
                        'cycle_date': date_formatted,
                        'cycle_hour': cycle_hour
                    })
                    out_f.write(record + '\n')
                    record_count += 1

                print(f'Decoded {variable_name} ({type_of_level}={level}): {len(values)} grid points')

            finally:
                eccodes.codes_release(gid)

    print(f'Total records: {record_count}, skipped messages: {skipped_count}')
    return record_count


if __name__ == "__main__":
    result = handler({}, None)
    print(json.dumps(result))
