var express = require('express');
var router = express.Router();
require('dotenv').config()
var h1 = process.env.CASSANDRA_HOST1;
var h2 = process.env.CASSANDRA_HOST2;
const cassandra = require('cassandra-driver');
const campsitesClient = new cassandra.Client({ contactPoints: [h1, h2], keyspace: 'campsites' });
const stationsClient = new cassandra.Client({ contactPoints: [h1, h2], keyspace: 'weather_stations' });

function nearestHours(date) {
  /**
   * Finds the hours that bound a date, i.e., the hour before and hour after.
   *
   * @param {Date} date - A date object for which to find the bounding hours
   * @returns {Object} - An object with a firstNearestHour and secondNearestHour
   *                     attribute, which are Date objects that correspond to
   *                     the hour before and the hour after the given date. If
   *                     the given date falls on an hour, these are identical.
   *                     Also returns firstDelta and secondDelta, which are the
   *                     number of minutes of the given Date to the first hour
   *                     and the second hour.
   */
  var year = date.getUTCFullYear();
  var month = date.getUTCMonth();
  var day = date.getUTCDate();
  var hours = date.getUTCHours();
  var minutes = date.getUTCMinutes();
  
  var secondNearestHour = 0;
  if (date.getMinutes() === 0) {
    secondNearestHour = date.getUTCHours()
  }
  else {
    secondNearestHour = date.getUTCHours() + 1
  }
  var firstDelta = date.getMinutes();
  var secondDelta = 60 - date.getUTCMinutes();
  if (secondDelta === 60) {
    secondDelta = 0;
  }

  var nearestHoursObj = {};
  nearestHoursObj.firstNearestHour = new Date(Date.UTC(year, month, day, hours, 0));
  nearestHoursObj.secondNearestHour = new Date(Date.UTC(year, month, day, secondNearestHour, 0));
  nearestHoursObj.firstDelta = firstDelta;
  nearestHoursObj.secondDelta = secondDelta;
  return nearestHoursObj
}

function calcTimeWeightedAverage(nearestHourObj, temp1, temp2) {
  /**
   * Calculates time weighted average temperature
   *
   * @param {Object} nearestHourObj - Object from nearestHours function
   * @param {Float} temp1 - Temperature at first hour
   * @param {Float} temp2 - Temperature at second hour
   * @returns {Float} - Time weighted average temperature
   */
  var numerator = temp1 * nearestHourObj.firstDelta + temp1 * nearestHourObj.secondDelta;
  var denominator = nearestHourObj.firstDelta + nearestHourObj.secondDelta;
  return numerator / parseFloat(denominator);
}

router.post('/get_hist_campsite_weather', function(req, res, next) {
  // Returns the weather for a campsite at a given date/time
  // Assume that date is in milliseconds after epoch
  
  // TODO: Error handling here in case these portions of the body are not available
  // TODO: Make it only possible to query for hourly data
  var milliseconds_date = parseInt(req.body.date);
  var date = new Date(milliseconds_date);
  var facilityId = parseInt(req.body.facilityId);
  
  const query = "SELECT * FROM campsites.calculations WHERE campsite_id = ? AND calculation_time = ?";
  var nearestHoursObj = nearestHours(date)
  // TODO: Handle the case where there is no response, i.e., no rows that match
  if (nearestHoursObj.firstDelta === 0) {
    // Just need one query in this case
    campsitesClient.execute(query, [facilityId, milliseconds_date], {prepare: true})
    .then(result => {
      var resultData = {facilityId: result.rows[0].campsite_id, temp: result.rows[0].temp};
      res.json(resultData);
    },
    error => {
      next(error);
    });
  }
  
  res.render('index');
});

module.exports = router;
