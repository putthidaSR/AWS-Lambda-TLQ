package uwt.model;

public class QueryResult {
	
    private String query;
    private double value;

    public QueryResult() {
    }

    public QueryResult(String query) {
        this();
        this.query = query;
    }

    public QueryResult(String query, double value) {
        this(query);
        this.value = value;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}