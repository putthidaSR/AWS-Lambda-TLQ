package uwt.lambda;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import uwt.exception.DataTransformationException;
import uwt.inspector.Inspector;
import uwt.model.Request;
import uwt.model.Response;

/**
 * The purpose of this class is to read the input CSV file from Amazon S3,
 * perform data transformation process, and write the newly created file with
 * transformed data to Amazon S3.
 */
public class TransformService implements RequestHandler<Request, HashMap<String, Object>> {

	private LambdaLogger logger; // Lambda runtime logger

	/**
	 * Lambda Function Handler to retrieve file from Amazon S3, perform data
	 * transformation process, and write the file with transformed data to S3.
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

		// Collect inital data.
		Inspector inspector = new Inspector();
		inspector.inspectAll();

		String bucketName = request.getBucketName();
		String fileName = request.getFileName();
		logger.log(">>> " + bucketName + ", " + fileName);

		// Get the object file from S3
		logger.log(String.format("Attempt to read file [%s] from S3 bucket: %s", fileName, bucketName));
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
		S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
		logger.log(String.format("Retrieve file [%s] from S3 bucket: %s", fileName, bucketName));

		// Get content of the file
		S3ObjectInputStream objectData = s3Object.getObjectContent();

		// Transform the input data and return character stream that will be used to
		// write a new file
		StringWriter outputFile = transformData(objectData);

		// Create metadata for describing the file to be written to S3 and create the
		// new file on Amazon S3
		byte[] bytes = outputFile.toString().getBytes(StandardCharsets.UTF_8);
		InputStream inputStream = new ByteArrayInputStream(bytes);
		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(bytes.length);
		meta.setContentType("text/plain");

		String newFileNameAfterDataTransform = "TransformedData";
		logger.log(String.format("Attempt to create new file [%s] on S3 bucket [%s]", newFileNameAfterDataTransform,
				bucketName));

		// Create new file on S3
		s3Client.putObject(bucketName, newFileNameAfterDataTransform, inputStream, meta);

		// Create and populate a separate response object for function output
		Response response = new Response();
		response.setValue("Bucket: " + bucketName + ", filename: " + newFileNameAfterDataTransform + " processed.");
		logger.log("Finished creating new file on S3");

		// Add all attributes of a response object to FaaS Inspector
		inspector.consumeResponse(response);

		// Collect final information such as total runtime and cpu deltas.
		inspector.inspectAllDeltas();
		logger.log("Finished data transformation service");
		return inspector.finish();

	}

	/**
	 * Transforms the contents of the CSV file and return character streams of the
	 * transformed data.
	 * 
	 * @param objectData
	 *            Contents from the original CSV file
	 * @return String of transformed data
	 */
	private StringWriter transformData(InputStream objectData) {

		StringWriter outputFile = new StringWriter();

		// Scanning data line by line
		Scanner scanner = new Scanner(objectData);
		String line = scanner.nextLine();

		// Add column [Order Processing Time] at the end of first row
		line += ",Order Processing Time,Gross Margin\n";
		outputFile.append(line);

		// Set of unique Order ID (to be used to detect Order ID duplication)
		HashSet<Long> orderIdSet = new HashSet<>();

		while (scanner.hasNext()) {

			line = scanner.nextLine();
			String[] token = line.split(",");

			// Detect duplication in current record. Only proceed with unique order ID.
			long orderId = Long.parseLong(token[6]);
			if (orderIdSet.contains(orderId)) {
				logger.log(String.format("Detect dupilcation for order ID [%d]. Continue with the next order ID.",
						orderId));
				continue;
			}
			orderIdSet.add(orderId);

			// Calculate order processing days
			long processingDays = getNumberOfProccessingDays(token[5], token[7]);
			logger.log("Order processing days: " + processingDays);

			// Calculate Gross Margin
			double grossMargin = getGrossMargin(token[token.length - 1], token[token.length - 3]);
			logger.log("Gross margin: " + grossMargin);

			// Transform order priority value
			String newOrderPriortyValue = transformOrderPriority(token[4]);
			token[4] = newOrderPriortyValue;

			// Convert to String and write back to StringWriter
			line = String.join(",", token);
			String lastToken = String.format(",%d,%.2f\n", processingDays, grossMargin);

			line += lastToken;
			logger.log("Line to be written to the file after transformation:\n" + line);

			outputFile.append(line);
		}
		scanner.close();

		return outputFile;
	}

	/**
	 * Calculate the order processing time.
	 * 
	 * @param orderDateString
	 *            Represents item ordered date
	 * @param shipDateString
	 *            Represents item shipped date
	 * @return number of days between order date and ship date
	 * @throws DataTransformationException
	 *             when failing to parse date format
	 */
	private long getNumberOfProccessingDays(final String orderDateString, final String shipDateString) {

		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		Date orderDate = null;
		Date shipDate = null;

		try {

			orderDate = dateFormat.parse(orderDateString);
			shipDate = dateFormat.parse(shipDateString);

		} catch (ParseException e) {

			logger.log(String.format(DataTransformationException.FAIL_DATE_PARSING + " with exception: %s", e));
			throw new DataTransformationException(DataTransformationException.FAIL_DATE_PARSING, e);
		}

		long timeDifferenceInMilliSeconds = shipDate.getTime() - orderDate.getTime();
		return TimeUnit.MILLISECONDS.toDays(timeDifferenceInMilliSeconds);
	}

	/**
	 * Calculate the gross margin.
	 * 
	 * @param profitString
	 *            Value of profit retrieves from CSV file
	 * @param revenueString
	 *            Value of revenue retrieves from CSV file
	 * @return value of gross margin of the specified order
	 */
	private double getGrossMargin(final String profitString, final String revenueString) {

		double profit = Double.parseDouble(profitString);
		double revenue = Double.parseDouble(revenueString);

		return profit / revenue;
	}

	/**
	 * Transform [Order Priority] value: 
	 * L to “Low” 
	 * M to “Medium” 
	 * H to “High” 
	 * C to “Critical”
	 * 
	 * @param initialOrderPriorityValue
	 *            Order priority value retrieves from CSV file
	 * @return Order priority value after transformation
	 * @throws DataTransformationException
	 *             when failing to identify the original order priority
	 */
	private String transformOrderPriority(final String initialOrderPriorityValue) {

		switch (initialOrderPriorityValue) {
		case "L":
			return "Low";
		case "M":
			return "Medium";
		case "H":
			return "High";
		case "C":
			return "Critical";
		default:
			throw new DataTransformationException(DataTransformationException.FAIL_ORDER_PRIORITY_PARSING);
		}

	}

}
