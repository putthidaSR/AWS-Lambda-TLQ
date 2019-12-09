package uwt.lambda;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;

import uwt.inspector.Inspector;
import uwt.model.QueryResult;
import uwt.model.Request;
import uwt.model.Response;


/**
 * The purpose of this class is to perform filtering and aggregation of data queries on data loaded into SQLite relational database.
 */
public class QueryService implements RequestHandler<Request, HashMap<String, Object>> {

	/**
	 * Lambda Function Handler to perform filtering and aggregation of data queries on the local database SQLite.
	 * 
	 * @param request
	 *            The request POJO with defined variables from Request.java
	 * @param context
	 *            The context object to access information available within the
	 *            Lambda execution environment
	 * @return HashMap that Lambda will automatically convert into JSON
	 */
	@Override
	public HashMap<String, Object> handleRequest(Request request, Context context) {

		// Create logger
		LambdaLogger logger = context.getLogger();
		logger.log("Begin data transformation service");

		// Register function
		Inspector inspector = new Inspector();
		inspector.inspectAll();

		setCurrentDirectory("/tmp");
				
		/**
		 * TODO:
		 * - Detect database called SaleRecords from S3: SaleRecords DB must exist at this point (created in Service #2 - Load)
		 * Connect to SQLite DB
		 * - Detect database called SaleRecords (Order_ID is primary key)
		 * - Create query based on the request parameters from bash script
		 * - Write query result as JSON response output
		 * - Display JSON response in terminal
		 * - Consider write the JSON response output to a file and upload to S3
		 */

		String filter =  request.getFilter();
        String aggregation =  request.getAggregation();
        
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        String[] names = filename.split("/");
        String dbname = names[names.length - 1];

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        
        // Get db file using source bucket and srcKey name and save to /tmp
        File file = new File("/tmp/" + dbname);
        s3Client.getObject(new GetObjectRequest(bucketname, filename), file);
        
        // StringBuilder sb = new StringBuilder();
        List<QueryResult> results = new LinkedList<>();
        
        try {
        	
            // Connection string for a file-based SQlite DB
            Connection con = DriverManager.getConnection("jdbc:sqlite:" + dbname); 

            // Detect if the table 'salesrecords' exists in the database
            PreparedStatement ps = con.prepareStatement(
            		"SELECT name FROM sqlite_master WHERE type='table' AND name='salesrecords'"
                );
            
            ResultSet rs = ps.executeQuery();
            
            if (!rs.next()) {
                // 'salesrecords' does not exist, throw exception
                logger.log("No such table: 'salesrecords'");
                throw new SQLException("No such table: 'salesrecords'");
            }
            rs.close();
            
            // create query from request
            String query = "";
            // Check if filter argument is passed
            if (filter == null || filter.equals("") || filter.equals("*")) {
                query = "SELECT " + aggregation + " FROM salesrecords";
            } else { 
                query = "SELECT " + aggregation + " FROM salesrecords WHERE " + filter;
            }
            ps = con.prepareStatement(query);
            rs = ps.executeQuery();

			// Write query result to output
			String[] aggregations = aggregation.split(",");
			if (rs.next()) {
				for (int i = 0; i < aggregations.length; i++) {
					query = aggregations[i];
					double value = Double.parseDouble(rs.getString(i + 1));
					QueryResult queryResult = new QueryResult(query, value);
					results.add(queryResult);
				}
			} else {
				// No result when query with given filter
				logger.log("No result when query");
			}
			rs.close();
			con.close();
			
		} catch (SQLException sqle) {
			logger.log("DB ERROR:" + sqle.toString());
			sqle.printStackTrace();
		}
        
        
		// Create and populate a separate response object for function output
		Response response = new Response();
		response.setResults(results);
		
		// Add all attributes of a response object to FaaS Inspector
		inspector.consumeResponse(response);

		// Collect final information such as total runtime and cpu deltas.
		inspector.inspectAllDeltas();
		return inspector.finish();

	}
	
	/**
	 * Set current working directory.
	 * Source: https://github.com/wlloyduw/saaf_sqlite/blob/master/SAAF/java_template/src/main/java/lambda/HelloSqlite.java
	 * @param directory_name
	 * @return
	 */
	public static boolean setCurrentDirectory(String directory_name) {
		
		boolean result = false; // Boolean indicating whether directory was set
		File directory; // Desired current working directory

		directory = new File(directory_name).getAbsoluteFile();
		if (directory.exists() || directory.mkdirs()) {
			result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
		}

		return result;
	}

}
