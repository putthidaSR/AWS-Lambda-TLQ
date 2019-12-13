package uwt.model;

public class Request {

	private String bucketname;
	private String filename;
	private String filter;
	private String aggregation;

	public Request() {
	}

	public Request(String bucketname, String fileName, String filter, String aggregation) {
		this();
		this.bucketname = bucketname;
		this.filename = fileName;
		this.filter = filter;
		this.aggregation = aggregation;
	}

	public String getBucketname() {
		return bucketname;
	}

	public void setBucketname(String bucketname) {
		this.bucketname = bucketname;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public String getAggregation() {
		return aggregation;
	}

	public void setAggregation(String aggregation) {
		this.aggregation = aggregation;
	}
}
