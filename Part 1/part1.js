const querystring = require('querystring');
const https = require('https');
const fs = require('fs');

var endpoint = 'https://api.fda.gov/food/event.json';

function performRequest(start, end, success) {
  var headers = {};

  endpoint += '?search=date_started:[' + start + '+TO+' + end + ']&sort=date_started:asc&limit=100'; // always want to GET from here; no need to handle other request types or allow for other endpoints
  try {
    var request = https.request(endpoint, function(result) {
      result.setEncoding('utf-8');

      var responseString = '';

      result.on('data', function(data) {
        responseString += data;
      });

      result.on('end', function() {
        console.log(responseString);
        var responseObject = JSON.parse(responseString);
        console.log(result.headers.link)
        success(responseObject);
      });
    });


    var dataString = JSON.stringify({});
    request.write(dataString);
    request.end();
  } catch (err) {
    console.error('Error performing request: ' + err);
  }
}

async function retrieveEvents(CONCURRENCY_LIMIT, success) {
  var headers = {};
  var start = '20000101';
  var end = '20260101';
  var conc_limit = CONCURRENCY_LIMIT || 3;
  
  const events = await performRequest(start, end, success)
  console.log('AAAAAAAAAAAAA' + conc_limit)
}

function requestEvents() {
  try {
    const CONCURRENCY_LIMIT = Number.parseInt(process.argv[2]) || 3; // just in case something like "bc" or "14.3" is inputted in the command line
    retrieveEvents(CONCURRENCY_LIMIT, function(data) { // node part1.js # where # is the concurrency limit
      if (data.error !== undefined) {
        console.error('Error from API: ' + data.error.message);
        return;
      }
      console.log('Fetched ' + data.meta.results.total + ' adverse events');
      for (var i = 0; i < data.results.length; i++) {
        fs.writeFileSync('./data/event_' + data.results[i].report_number + '.json', JSON.stringify(data.results[i], null, 2));
      }
    });
  } catch (err) {
    console.error('Error retrieving events: ' + err);
  }
}

requestEvents();