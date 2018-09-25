package neo4j;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 25/06/2018.
 *
 * Class used to return a relation from the query result.
 */
public class ResultRelation extends ResultEntity {

	private long startNodeId;
	private long endNodeId;
	private HashMap<String, Object> properties;


	ResultRelation() {
		super();
		this.properties = new HashMap<>();
	}

	void addProperty(String propertyKey, Object value) {
		properties.put(propertyKey, value);
	}

	public HashMap<String, Object> getProperties() {
		return properties;
	}

	public long getStartNodeId() {
		return startNodeId;
	}

	void setStartNodeId(long startNodeId) {
		this.startNodeId = startNodeId;
	}

	public long getEndNodeId() {
		return endNodeId;
	}

	void setEndNodeId(long endNodeId) {
		this.endNodeId = endNodeId;
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();

		strBuilder.append("[");

		// Properties
		for (Object o : getProperties().entrySet()) {
			Map.Entry entry = (Map.Entry) o;
			strBuilder.append(" ");
			strBuilder.append(entry.getKey());
			strBuilder.append(":");
			strBuilder.append(entry.getValue());
			strBuilder.append(",");
		}

		strBuilder.replace(strBuilder.length() - 1, strBuilder.length(), "]");

		return strBuilder.toString();
	}
}