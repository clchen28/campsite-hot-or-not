function degCtoDegF(degC) {
  return degC * 1.8 + 32;
}

function queryWeatherAtMarker(marker, map, time) {
  let facilityId = marker.facilityId;
  let data = {facilityId: facilityId, date: time};
  $.ajax({
    type: "POST",
    url: "/api/get_hist_campsite_weather",
    data: data,
    success: function(resp) {
      // TODO: Put the response data into the info window
      marker.results = degCtoDegF(resp.temp);
      let contentString = marker.contentString + "<br />" + marker.results.toPrecision(2) + " F";
      var infowindow = new google.maps.InfoWindow({
        content: contentString
      });
      infowindow.open(map, marker);
    },
    dataType: "json"
  });
}

function initMap() {
  $('#datetimepicker13').datetimepicker({
      defaultDate: "2016-01-24T19:00:00Z",
      inline: true,
      sideBySide: true,
      keepInvalid: false
  });
  // TODO: How to detect if there is an open infoWindow?
  // TODO: Way to remove data from markers on change in datetime?
  // $('#datetimepicker13').on('change.datetimepicker', function(e){console.log(e);})
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
      var markers = campgrounds.map(function(campground, i) {
        var contentString = "<h1>" + campground.name + "</h1><br />";
        contentString += "ID: " + campground.facilityId.toString() + "<br />";
        contentString += "Position: " + campground.position.lat.toString() + ", ";
        contentString += campground.position.lng.toString();
        /*
        var infowindow = new google.maps.InfoWindow({
          content: contentString
        });
        */

        var marker = new google.maps.Marker({
          position: campground.position,
          map: map,
          name: campground.name,
          facilityId: campground.facilityId,
          contentString: contentString,
          results: ""
        });

        marker.addListener('click', function() {
          // TODO: Also execute a request for the weather here

          // Get milliseconds after UNIX epoch
          var time = $('#datetimepicker13').datetimepicker('date').unix() * 1000;
          queryWeatherAtMarker(marker, map, time);
        });

        return marker;
      });
      // Add a marker clusterer to manage the markers.
      var markerCluster = new MarkerClusterer(map, markers,
      {imagePath: '/markerclusterer/images/m'});
    });
  }