package uwt.model;

public class Response {

	private String value;
	private String queryJsonOutput;

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getQueryJsonOutput() {
		return queryJsonOutput;
	}
	
	public void setQueryJsonOutput(String queryJsonOutput) {
		this.queryJsonOutput = queryJsonOutput;
	}

	@Override
	public String toString() {
		return "DB result in JSON = " + this.getQueryJsonOutput() + super.toString();
	}

}
