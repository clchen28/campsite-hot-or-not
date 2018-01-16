import json

output_obj = {}
with open("isd-history.txt") as f:
    for line in f:
        lat = line[57:64]
        lon = line[65:73]
        USAF = line[0:6]
        WBAN = line[7:12]
        try:
            float(lat)
            float(lon)
            int(USAF)
            int(WBAN)
        except:
            continue
        output_obj[str(USAF) + "|" + str(WBAN)] = {"lat": lat, "lon": lon}

with open("stations_latlon.json", "w") as output:
    output.write(json.dumps(output_obj))