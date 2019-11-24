package lambda;

import java.io.File;
import java.util.HashMap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import inspector.Inspector;
import model.Request;
import model.Response;


/**
 * The purpose of this class is to perform filtering and aggregation of data queries on data loaded into SQLite relational database.
 */
public class LoadService implements RequestHandler<Request, HashMap<String, Object>> {

	private LambdaLogger logger; // Lambda runtime logger

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
		logger = context.getLogger();
		logger.log("Begin data filtering and aggregation service");

		// Collect inital data.
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

		// Create and populate a separate response object for function output
		Response response = new Response();

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
