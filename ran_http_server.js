var util = require('util');
var http = require('http'); 
var mysql = require('mysql');
var EventEmitter = require('events').EventEmitter;
var server = http.createServer(); 
var port = 8080; 

var q1 = "select count(distinct uw.ticker) as no_of_deals, t.industry as industry " +
			"from b_bond_issue_underwriter_no_dupes uw " +
			"join b_bond_chain as bc on uw.ticker = bc.alt_bloom_id join sic_per_ticker b " +
			"on bc.ticker = b.ticker " +
			"join sic_ticker t on t.sic_code= left(b.sic,2) where uw.mngr_cde='CS' " +
			"group by left(b.sic,2) order by no_of_deals desc";

var pool =  mysql.createPool({
	host : '127.0.0.1',
  	port : 3306,
  	database: 'rantime',
  	user : 'username',
  	password : 'pass'
});

// on listener to request events
server.on('request', function(req, res) {

	console.log('Request came in: ' + logDate());
	
	// DB connection
	 pool.getConnection(function(err, connection){
  		
  		// select DB to use
  		connection.query('use rantime');  		
  		
  		// run query, switch on results what to display
		connection.query(q1, function(err, rows){
	  		if(err)	{
	  			displayError(res, err);
	  			throw err;
	  		}
	  		else{
	  			if(rows.length > 0){ displayResults(res, rows); }
	  			else{ displayEmptyResults(res); }
	  		}
	  	});

		// return connection back to pool
	  	connection.release();

  	});

}); 

// Start server and one time messags
server.listen(port); 
server.once('listening', function() {
	console.log('Hello World server listening on port %d', port); 
});

/* For logging and date time display */
function logDate(){

	var currentdate = new Date(); 
    var datetime = currentdate.getDate() + "/"
                + (currentdate.getMonth()+1)  + "/" 
                + currentdate.getFullYear() + "  "  
                + currentdate.getHours() + ":"  
                + currentdate.getMinutes() + ":" 
                + currentdate.getSeconds();

    return datetime;
}

/* Common header for all results */
function displayHeader(res){
	res.writeHead(200, {'Content-Type': 'text/html'});
	res.write('<html>\n<head>\n<title>RAN Test</title>\n</head>');

   	res.write('\n<h2>Hello Node World:</h2>');
	res.write('\n<h3>' + logDate() + '</h3>');
}

/* Common footer for all results */
function displayFooter(res){
	res.write('\n</body>\n</html>');	
	res.end(); 
}

/* Expected outcome when you have a list of items */
function displayResults(res, rows){
   	
   	console.log( rows );
	displayHeader(res);  			
	
	res.write('\nLen = ' + rows.length);
	for (var i = 0; i < rows.length; i++) {
		res.write('\n<br/>' + rows[i].no_of_deals + ' : ' + rows[i].industry );
	}
	  
	displayFooter(res);		
	
 }

/* No errors, results return 0 rows */
function displayEmptyResults(res, rows){
   			
   	displayHeader(res);
	res.write('No results found');	  			
	displayFooter(res);	
 }

/* DB error occurred, connection issues, etc.*/
function displayError(res, err){
   	  		
   	displayHeader(res);
	res.write('An error occured<br/>' + err);	  		
	displayFooter(res);	
 }


// Not needed but shows the server is running from the command line
var emitter = new EventEmitter(); 
var count = 0; 

setInterval(function() {
	emitter.emit('tick', count); 
	count ++; 
}, 10000); 

emitter.on('tick', function(count) {
	console.log('tick:', count); 
});
