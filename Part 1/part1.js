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
            if (responseObject.results !== undefined) {
              console.log('Received ' + responseObject.results.length + ' events from API');
            }
            
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
  var conc_limit = Math.max(1, CONCURRENCY_LIMIT) || 3;
  
  try {
    // Request queue
    const requestQueue = [];
    let activeCount = 0;
    
    // Parse dates properly to handle month/day boundaries
    function dateStringToDate(dateStr) {
      const year = parseInt(dateStr.substring(0, 4));
      const month = parseInt(dateStr.substring(4, 6)) - 1; // months are 0-indexed
      const day = parseInt(dateStr.substring(6, 8));
      return new Date(year, month, day);
    }
    
    function dateToDateString(date) {
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      return `${year}${month}${day}`;
    }
    
    // Convert date strings to Date objects for proper arithmetic
    const startDate = dateStringToDate(start);
    const endDate = dateStringToDate(end);
    const totalMs = endDate.getTime() - startDate.getTime();
    const chunkMs = Math.ceil(totalMs / conc_limit);
    
    // Queue initial requests for each date chunk
    for (let i = 0; i < conc_limit; i++) {
      const chunkStartDate = new Date(startDate.getTime() + (i * chunkMs));
      let chunkEndDate = new Date(startDate.getTime() + ((i + 1) * chunkMs));
      
      // Make sure last chunk doesn't exceed end date
      if (chunkEndDate.getTime() > endDate.getTime()) {
        chunkEndDate = new Date(endDate);
      }
      
      if (chunkStartDate.getTime() >= endDate.getTime()) {
        break; // Don't create more chunks than needed
      }
      
      const chunkStart = dateToDateString(chunkStartDate);
      const chunkEnd = dateToDateString(chunkEndDate);
            
      const initialLink = endpoint + '?api_key=joKObkCY2YiDI1x0ENw18Rptd3Scr2Lh0dICGXF0&search=date_started:[' + chunkStart + '+TO+' + chunkEnd + ']&sort=date_started:asc&limit=500';
      
      requestQueue.push({
        link: initialLink,
        requestNumber: i + 1,
        chunkStart,
        chunkEnd
      });
    }
    
    // Worker function that processes requests from the queue
    const worker = async () => {
      while (requestQueue.length > 0) {
        const item = requestQueue.shift();
        const { link, requestNumber, chunkStart, chunkEnd } = item;
        
        try {
          console.log('[Request ' + requestNumber + '] Fetching');
          const response = await performRequest(link, success);
          
          if (!response.error && response.nextLink) {
            // Add next page to queue
            requestQueue.push({
              link: response.nextLink,
              requestNumber,
              chunkStart,
              chunkEnd
            });
          } else if (response.error) {
            console.error('[Request ' + requestNumber + '] API error: ' + response.error);
          }
        } catch (err) {
          console.error('[Request ' + requestNumber + '] Error: ' + err);
        }
      }
    };
    
    // Start concurrent workers
    const workers = [];
    for (let i = 0; i < conc_limit; i++) {
      workers.push(worker());
    }
    
    // Wait for all workers to complete
    await Promise.all(workers);
    return { status: 'completed' };
  } catch (err) {
    console.error('Error while retrieving events: ' + err);
    return { status: 'failed', error: err };
  }
}

async function requestEvents() {
  try {
    // Ensure data directory exists
    const dataDir = path.join(__dirname, '../data');
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
        if (data.results !== undefined) {
          for (let c = 0; c < data.results.length; c++) {
            writeCount++;
            const reportNumber = data.results[c].report_number;
            const eventData = data.results[c];
            const filePath = path.join(dataDir, 'event_' + reportNumber + '.json');

            fs.writeFile(filePath, JSON.stringify(eventData, null, 2), (err) => {
              if (err) {
                console.error('Error writing file: ' + err);
              } else {
              }
              writeComplete++;
              // Resolve only when all writes complete
              if (writeComplete === writeCount) {
                resolve();
              }
            });
          }
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