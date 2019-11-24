package uwt.model;

public class Request {

	private String bucketName;
	private String fileName;
	
	public Request() {
		
	}
	
	public Request(String bucketName, String fileName) {
		this.bucketName = bucketName;
		this.fileName = fileName;
	}
	
	public String getBucketName() {
		return bucketName;
	}
	
	public String getFileName() {
		return fileName;
	}
	
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
