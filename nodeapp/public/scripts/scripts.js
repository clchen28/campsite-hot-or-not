function initMap() {
  var map = new google.maps.Map(document.getElementById('map'), {
    zoom: 3,
    center: {lat: 37.425713, lng: -122.1704554}
  });
    var campgrounds =  {};

    function loadJSON(callback) {   
      var xobj = new XMLHttpRequest();
      xobj.overrideMimeType("application/json");
      xobj.open('GET', '/campgrounds_info.json', true);
      xobj.onreadystatechange = function () {
            if (xobj.readyState == 4 && xobj.status == "200") {
              callback(xobj.responseText);
            }
      };
      xobj.send(null);  
    }

   loadJSON(function(response) {
      // Parse JSON string into object
      campgrounds = JSON.parse(response);
      campgrounds = campgrounds.campgrounds;
      console.log(campgrounds);
      var markers = campgrounds.map(function(campground, i) {
        var contentString = "<h1>" + campground.name + "</h1><br />";
        contentString += "ID: " + campground.facilityId.toString() + "<br />";
        contentString += "Position: " + campground.position.lat.toString() + ", ";
        contentString += campground.position.lng.toString();
        var infowindow = new google.maps.InfoWindow({
          content: contentString
        });

        var marker = new google.maps.Marker({
          position: campground.position,
          map: map,
          name: campground.name,
          facilityId: campground.facilityId
        });

        marker.addListener('click', function() {
          infowindow.open(map, marker);
        });

        return marker;
      });
      // Add a marker clusterer to manage the markers.
      var markerCluster = new MarkerClusterer(map, markers,
      {imagePath: '/markerclusterer/images/m'});
    });
  }