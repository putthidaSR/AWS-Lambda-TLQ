package model;

public class Response {

	private String value;

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "value=" + this.getValue() + super.toString();
	}

}
