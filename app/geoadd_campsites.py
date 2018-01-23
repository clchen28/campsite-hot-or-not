import redis
import json

def add_to_redis(campground, r_client):
	'''
	Adds the specified campground to Redis
	:param campgrond: Dict with the following keys: lat, lon, name, facilityId
	:param r_client:  Redis client pointing to redis instance to send data to
	'''
	lat = campground.get("lat", None)
	lon = campground.get("lon", None)
	name = campground.get("name", None)
	facilityId = campground.get("facilityId", None)

	if not lat or not lon or not name or not facilityId:
		return

	value = str(lon) + " " + str(lat) + " " + str(facilityId)
	r_client.geoadd("campgrounds", lon, lat, facilityId)
	r_client.set(facilityId, name)

if __name__ == "__main__":
	with open("campgrounds_min.json") as f:
		a = f.readline()
		campgrounds = json.loads(a)
		campgrounds = campgrounds["campgrounds"]
	r = redis.Redis(host='localhost', port=6379, db=0)
	for campground in campgrounds:
		add_to_redis(campground, r)
