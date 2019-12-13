package uwt.model;

import java.util.List;

public class Response {

	private String value;
	private List<QueryResult> results;

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public List<QueryResult> getResults() {
		return results;
	}

	public void setResults(List<QueryResult> results) {
		this.results = results;
	}

	@Override
	public String toString() {
		return "value = " + this.getResults() + super.toString();
	}

}
