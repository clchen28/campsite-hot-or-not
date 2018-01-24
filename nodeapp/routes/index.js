var express = require('express');
var router = express.Router();
require('dotenv').config()
var api_key = process.env.GOOGLE_MAPS_API_KEY;

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index');
});

module.exports = router;
