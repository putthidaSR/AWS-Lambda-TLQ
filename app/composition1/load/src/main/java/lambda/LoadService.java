package lambda;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import inspector.Inspector;
import model.Request;
import model.Response;


/**
 * The purpose of this class is to load the data from the CSV file hosted on S3 into a single table relational database 
 * (use locally hosted database SQLite).
 */
public class LoadService implements RequestHandler<Request, HashMap<String, Object>> {

	private LambdaLogger logger; // Lambda runtime logger

	/**
	 * Lambda Function Handler to load file from Amazon S3 and host it in the local database SQLite.
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
		logger = context.getLogger();
		logger.log("Begin data transformation service");

		// Collect inital data.
		Inspector inspector = new Inspector();
		inspector.inspectAll();

		setCurrentDirectory("/tmp");
		
		String bucketName = request.getBucketName();
		String fileName = request.getFileName();

		// TODO: Get the object file from S3 (naming from must match whatever specified in Transformation Service) 
		logger.log(String.format("Attempt to read file [%s] from S3 bucket: %s", fileName, bucketName));
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
		S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
		logger.log(String.format("Retrieve file [%s] from S3 bucket: %s", fileName, bucketName));

		// Get content of the file
		InputStream objectData = s3Object.getObjectContent();
				
		/**
		 * TODO:
		 * - Scanning data line by line
		 * - Create database called SaleRecords (Order_ID is primary key)
		 * - Scan each line from input CSV file (retrieves from S3) and insert it to SaleRecords DB row by row
		 * - Update the newly created DB as a file to S3
		 */

		// Create and populate a separate response object for function output
		Response response = new Response();
		response.setValue("Bucket: " + bucketName + ", filename: " + fileName + " processed.");
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
