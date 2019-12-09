package uwt.lambda;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Scanner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import uwt.inspector.Inspector;
import uwt.model.Request;
import uwt.model.Response;


/**
 * The purpose of this class is to load the data from the CSV file hosted on S3 into a single table relational database 
 * (use locally hosted database SQLite).
 */
public class LoadService implements RequestHandler<Request, HashMap<String, Object>> {
	
	/**
	 * Lambda Function Handler to load file from Amazon S3 and host it in the local database SQLite.
	 * 
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
		logger.log("Begin data load service");

		// Register function
		Inspector inspector = new Inspector();
		inspector.inspectAll();

		setCurrentDirectory("/tmp");

		/**
		 * TODO plan: 
		 * - Scanning data line by line 
		 * - Create database called SaleRecords
		 * (Order_ID is primary key) 
		 * - Scan each line from input CSV file (retrieves
		 * from S3) and insert it to SaleRecords DB row by row 
		 * - Update the newly created DB as a file to S3
		 */
		
		String bucketName = request.getBucketName();
		String fileName = request.getFileName();

		String dbname = "transformData.db";

		// TODO: Get the object file from S3 (naming from must match whatever specified
		// in Transformation Service)
		logger.log(String.format("Attempt to read file [%s] from S3 bucket: %s", fileName, bucketName));
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
		S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
		logger.log(String.format("Retrieve file [%s] from S3 bucket: %s", fileName, bucketName));

		// Get content of the file
		InputStream objectData = s3Object.getObjectContent();

		// Scan data line by line
		Scanner scanner = new Scanner(objectData);
		String line = scanner.nextLine();

		try {

			// Connection string for a file-based SQlite DB
			//Connection con = DriverManager.getConnection("jdbc:sqlite:" + dbname);
			
			String url = "jdbc:sqlite:transformData.db";
			Connection con = DriverManager.getConnection(url);
			
			logger.log("Connection to SQLite has been established.");

			// Detect if the table 'salesrecords' exists in the database
			PreparedStatement ps = con
					.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name='salesrecords'");

			ResultSet rs = ps.executeQuery();

			if (!rs.next()) {

				// 'salesrecords' does not exist, and should be created
				logger.log("Attempting to create table 'salesrecords'");

				ps = con.prepareStatement(
					"CREATE TABLE salesrecords (" + 
						"Region text," + 
						"Country text," + 
						"Item_Type text," + 
						"Sales_Channel text," + 
						"Order_Priority text," + 
						"Order_Date date," + 
						"Order_ID integer PRIMARY KEY," + 
						"Ship_Date date," + 
						"Units_Sold integer," + 
						"Unit_Price float," + 
						"Unit_Cost float," + 
						"Total_Revenue float," + 
						"Total_Cost float," + 
						"Total_Profit float," + 
						"Order_Processing_Time integer," + 
						"Gross_Margin float" + 
					");");

				ps.execute();
			}
			rs.close();

			ps = con.prepareStatement("PRAGMA synchronous = OFF;");
			ps.execute();

			ps = con.prepareStatement("BEGIN");
			ps.execute();
			ps.close();

			logger.log("DB insertion start.");
			// int count = 0;

			// Insert row into salesrecords
			while (scanner.hasNext()) {

				line = scanner.nextLine();
				line = line.replace("\'", "\'\'");
				String[] token = line.split(",");

				for (int i = 0; i < 5; i++)
					token[i] = "'" + token[i] + "'";

				String[] date = token[5].split("/");
				token[5] = "'" + date[2] + "-" + date[0] + "-" + date[1] + "'";
				date = token[7].split("/");
				token[7] = "'" + date[2] + "-" + date[0] + "-" + date[1] + "'";

				line = String.join(",", token);
				ps = con.prepareStatement("INSERT INTO salesrecords values(" + line + ");");
				ps.execute();
				ps.close();

				// if (count % 100000 == 0)
				// logger.log("10W records inserted");
				// count ++;
			}

			ps = con.prepareStatement("COMMIT");
			ps.execute();
			ps.close();

			con.close();
			logger.log("DB insertion end.");

		} catch (SQLException sqle) {
			logger.log("DB ERROR:" + sqle.toString());
			sqle.printStackTrace();
		}

		scanner.close();
		File file = new File("/tmp/" + dbname);

		//s3Client.putObject(new PutObjectRequest(bucketName, "SalesRecordsDB/" + dbname , file)); 
		s3Client.putObject(bucketName, "SalesRecordsDB/" + dbname, file);
		file.delete();

		// Create and populate a separate response object for function output
		Response response = new Response();
		
		String msg = "Bucket: " + bucketName + " filename: " + fileName + " loaded. DBname: " + dbname;
        logger.log(msg);
        response.setValue(msg);
		logger.log("Finished creating new file on S3");

		// Add all attributes of a response object to FaaS Inspector
		inspector.consumeResponse(response);

		// Collect final information such as total runtime and cpu deltas.
		inspector.inspectAllDeltas();
		logger.log("Finished data transformation service");
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
