package uwt.model;

public class Response {

	private String value;
	private long runtime;

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	public long getRuntime() {
		return runtime;
	}
	
	public void setRuntime(long runtime) {
		this.runtime = runtime;
	}

	@Override
	public String toString() {
		return "value=" + this.getValue() + super.toString();
	}

}
