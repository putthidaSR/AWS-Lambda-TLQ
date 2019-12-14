package uwt.model;

public class Response {

	private String value;
	private long transformRuntime;

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	public long getTransformRuntime() {
		return transformRuntime;
	}
	
	public void setTransformRuntime(long runtime) {
		this.transformRuntime = runtime;
	}

	@Override
	public String toString() {
		return "value=" + this.getValue() + super.toString();
	}

}
