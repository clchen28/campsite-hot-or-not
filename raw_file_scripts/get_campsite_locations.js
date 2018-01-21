var fs = require("fs");
var facilitiesRaw = require("./Facilities_API_v1.json")
var facilitiesList = facilitiesRaw.RECDATA;
var arrayLength = facilitiesList.length;

var campgrounds_list = [];

for (var i = 0; i < arrayLength; i++) {
    if (facilitiesList[i].FacilityTypeDescription === "Camping") {
        var lat = facilitiesList[i].FacilityLatitude;
        var lon = facilitiesList[i].FacilityLongitude;
        var name = facilitiesList[i].FacilityName;
        var facilityId = facilitiesList[i].FacilityID;
        var legacyFacilityId = facilitiesList[i].LegacyFacilityID;
        var campsite = {"name": name,
            "facilityId": facilityId,
            "legacyFacilityId": legacyFacilityId,
            "lat": lat,
            "lon": lon
        };
        campgrounds_list.push(campsite);
    }
}

var campgrounds = {"campgrounds": campgrounds_list};

fs.writeFile('./campgrounds.json', JSON.stringify(campgrounds, null, 2) , 'utf-8');