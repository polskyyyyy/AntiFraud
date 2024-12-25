import requests

BASE_URL = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{}-{:02d}.csv"
FILENAME_TEMPLATE = "data/yellow_tripdata_{}-{:02d}.csv"

start_year = 2016
end_year = 2016

start_month = 2
end_month = 6


while True:
    # Get current file url, and generate downloaded filename
    url = BASE_URL.format(start_year, start_month)
    cur_filename = FILENAME_TEMPLATE.format(start_year, start_month)

    print(url, cur_filename)

    # Download and write file
    r = requests.get(url)
    with open(cur_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

    start_month += 1
    if(start_month > 12):
        start_month = 1
        start_year += 1

    if(start_year == end_year and start_month == end_month + 1):
        break

print("DONE")