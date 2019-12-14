package uwt.lambda;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.util.StringUtils;

import uwt.inspector.Inspector;
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
		logger.log("Begin data query service");

		// Register function
		Inspector inspector = new Inspector();
		inspector.inspectAll();

		setCurrentDirectory("/tmp");
     	
		/**
		 * Implementation steps: 
		 * 
		 * - Detect database called SaleRecords from S3: SaleRecords DB must exist
		 * at this point (created in Service #2 - Load) Connect to SQLite DB 
		 * - Detect database called SaleRecords (Order_ID is primary key) - Create query based on
		 * the request parameters from bash script - Write query result as JSON response
		 * output - Display JSON response in terminal - Consider write the JSON response
		 * output to a file and upload to S3
		 */

		String filter = request.getFilter();
		String aggregation = request.getAggregation();
		String groupBy = request.getGroupBy();

		String bucketname = request.getBucketname();
		String filename = request.getFilename();
		String dbname = "transformData.db";

		JSONArray jsonArray = new JSONArray();

		try {

			// Connection string for a file-based SQlite DB
			Connection con = DriverManager.getConnection("jdbc:sqlite:" + dbname);

			logger.log("Connection to SQLite has been established.");

			// Detect if the table 'salesrecords' exists in the database
			PreparedStatement ps = con
					.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name='salesrecords'");

			ResultSet rs = ps.executeQuery();

			if (!rs.next()) {
				// 'salesrecords' does not exist, read from S3
				logger.log("No such table: 'salesrecords'");

				logger.log(String.format("Attempt to read file [%s] from S3 bucket: %s", filename, bucketname));
				AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

				// Get db file using source bucket and srcKey name and save to /tmp
				File file = new File("/tmp/" + dbname);
				s3Client.getObject(new GetObjectRequest(bucketname, filename), file);

				logger.log(String.format("Retrieve file [%s] from S3 bucket: %s", filename, bucketname));
			}
			rs.close();

			logger.log("Begin data filtering and aggregation");

			// create query from request
			String query = "";

			// Check if filter argument is passed
			if (StringUtils.isNullOrEmpty(filter) || filter.equals("*")) {
				query = "SELECT " + aggregation + " FROM salesrecords";
			} else {
				query = "SELECT " + aggregation + " FROM salesrecords WHERE " + filter;
			}

			// Append groupBy if the agrument is passed
			if (!StringUtils.isNullOrEmpty(groupBy)) {
				query = query + " GROUP BY " + groupBy;
			}

			ps = con.prepareStatement(query);
			rs = ps.executeQuery();
			logger.log("RESULTS: ---");

			jsonArray = convertToJSON(rs);
			logger.log(jsonArray.toString(4));

			rs.close();
			con.close();

		} catch (SQLException sqle) {
			logger.log("DB ERROR:" + sqle.toString());
			sqle.printStackTrace();
		} catch (Exception e) {
			logger.log("EXCEPTION THROWN:" + e.toString());
		}

		logger.log("RESULTS:");
		logger.log(jsonArray.toString(4));

		// Create and populate a separate response object for function output
		Response response = new Response();
		response.setQueryJsonOutput(jsonArray.toString(4));

		// Add all attributes of a response object to FaaS Inspector
		inspector.consumeResponse(response);

		// Collect final information such as total runtime and cpu deltas.
		inspector.inspectAllDeltas();
		logger.log("Finished data filtering and aggregation service");

		return inspector.finish();

	}
	
	/**
	 * Convert ResultSet in DB format to JSON
	 * Source: http://biercoff.com/nice-and-simple-converter-of-java-resultset-into-jsonarray-or-xml/
	 */
	public static JSONArray convertToJSON(ResultSet resultSet) {
		JSONArray jsonArray = new JSONArray();
		try {
			while (resultSet.next()) {
				int total_rows = resultSet.getMetaData().getColumnCount();
				for (int i = 0; i < total_rows; i++) {
					JSONObject obj = new JSONObject();
					obj.put(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), resultSet.getObject(i + 1));
					jsonArray.put(obj);
				}
			}
		} catch (JSONException | SQLException e) {
			e.printStackTrace();
		}
		return jsonArray;
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
