const querystring = require('querystring');
const https = require('https');
const fs = require('fs');
const path = require('path');
var newLink = null;
var j = 0;
var k = 0;
var stop = false;
var size = 0;
var start = '20000101';
var end = '20260101';


var endpoint = 'https://api.fda.gov/food/event.json';

function performRequest(link, success) {
  return new Promise((resolve, reject) => {
    try {
      var request = https.request(link, function(result) {
        result.setEncoding('utf-8');

        var responseString = '';

        result.on('data', function(data) {
          responseString += data;
        });

        result.on('end', function() {
          try {
            var responseObject = JSON.parse(responseString);
            const linkHeader = result.headers.link;
            console.log('Received ' + responseObject.results.length + ' events from API');
            
            // Wait for success callback to complete before resolving
            Promise.resolve(success(responseObject)).then(() => {
              // Extract next page link if it exists
              let nextLink = null;
              if (linkHeader) {
                const matches = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
                if (matches && matches[1]) {
                  nextLink = matches[1];
                }
              }
              
              resolve({ data: responseObject, nextLink: nextLink });
            }).catch((err) => {
              console.error('Error in success callback: ' + err);
              resolve({ error: err, nextLink: null });
            });
          } catch (parseErr) {
            console.error('Error parsing response: ' + parseErr);
            resolve({ error: parseErr, nextLink: null });
          }
        });
      });

      request.on('error', (err) => {
        console.error('Request error: ' + err);
        resolve({ error: err, nextLink: null });
      });

      var dataString = JSON.stringify({});
      request.write(dataString);
      request.end();
    } catch (err) {
      console.error('Error performing request: ' + err);
      resolve({ error: err, nextLink: null });
    }
  });
}

async function retrieveEvents(CONCURRENCY_LIMIT, success) {
  var conc_limit = CONCURRENCY_LIMIT || 3;
  
  try {
    // Convert date strings to numbers for arithmetic
    const startNum = Number.parseInt(start);
    const endNum = Number.parseInt(end);
    const dateRange = endNum - startNum;
    const chunkSize = Math.ceil(dateRange / conc_limit);
    
    const eventRetrievers = [];
    
    // Create concurrent requests for each date chunk with pagination support
    for (let i = 0; i < conc_limit; i++) {
      const chunkStart = (startNum + (i * chunkSize)).toString();
      let chunkEnd = (startNum + ((i + 1) * chunkSize)).toString();
      
      // Make sure last chunk doesn't exceed end date
      if (Number.parseInt(chunkEnd) > endNum) {
        chunkEnd = end;
      }
      
      if (Number.parseInt(chunkStart) >= endNum) {
        break; // Don't create more chunks than needed
      }
      
      console.log('Creating request for date range: ' + chunkStart + ' to ' + chunkEnd);
      
      const retrieverPromise = (async () => {
        try {
          let currentLink = endpoint + '?api_key=3YnAUXIrSQccKmH7VYll8N5SLvBWgCVxPPEGAlzW&search=date_started:[' + chunkStart + '+TO+' + chunkEnd + ']&sort=date_started:asc&limit=100';
          let totalEventsFetched = 0;
          
          // Follow pagination links until no more pages
          while (currentLink) {
            console.log('Fetching: ' + currentLink);
            const response = await performRequest(currentLink, success);
            
            if (response.error) {
              console.error('Error fetching page: ' + response.error);
              break;
            }
            
            if (response.data && response.data.results) {
              totalEventsFetched += response.data.results.length;
              console.log('Total events fetched for this chunk so far: ' + totalEventsFetched);
            }
            
            // Move to next page if available
            currentLink = response.nextLink;
          }
          
          return { status: 'completed', dateRange: chunkStart + '-' + chunkEnd, totalEvents: totalEventsFetched };
        } catch (err) {
          console.error('Error retrieving events for ' + chunkStart + '-' + chunkEnd + ': ' + err);
          return { status: 'failed', error: err };
        }
      })();
      
      eventRetrievers.push(retrieverPromise);
    }
    
    console.log('Executing ' + eventRetrievers.length + ' concurrent date chunk requests');
    return await Promise.all(eventRetrievers);
  } catch (err) {
    console.error('Error in retrieveEvents: ' + err);
    return { status: 'failed', error: err };
  }
}

async function requestEvents() {
  try {
    // Ensure data directory exists
    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }

    const CONCURRENCY_LIMIT = Number.parseInt(process.argv[2]) || 3; // just in case something like "bc" or "14.3" is inputted in the command line
    
    // Create a success callback that returns a promise
    const successCallback = (data) => {
      return new Promise((resolve) => {
        if (data.error !== undefined) {
          console.error('Error from API: ' + data.error.message);
          resolve();
          return;
        }
        console.log('Fetched ' + data.meta.results.total + ' adverse events');
        
        // Track file writes
        let writeCount = 0;
        let writeComplete = 0;
        
        for (let c = 0; c < data.results.length; c++) {
          writeCount++;
          const reportNumber = data.results[c].report_number;
          const eventData = data.results[c];
          const filePath = path.join(dataDir, 'event_' + reportNumber + '.json');
          
          fs.writeFile(filePath, JSON.stringify(eventData, null, 2), (err) => {
            if (err) {
              console.error('Error writing file: ' + err);
            } else {
              console.log('Successfully wrote: ' + filePath);
            }
            writeComplete++;
            // Resolve only when all writes complete
            if (writeComplete === writeCount) {
              resolve();
            }
          });
        }
        
        // If no results, resolve immediately
        if (writeCount === 0) {
          resolve();
        }
      });
    };
    
    await retrieveEvents(CONCURRENCY_LIMIT, successCallback);
  } catch (err) {
    console.error('Error retrieving events: ' + err);
  }
}

requestEvents();