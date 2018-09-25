package neo4j;

import constants.GenericConstants;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 25/06/2018.
 *
 * Represents a Node inside the ResultQuery structure.
 */
public class ResultNode extends ResultEntity implements Serializable {
	private List<String> labels;
	private HashMap<String, Object> properties;


	ResultNode() {
		super();
		this.labels = new ArrayList<>();
		this.properties = new HashMap<>();
	}

	void addProperty(String propertyKey, Object value) {
		properties.put(propertyKey, value);
	}

	public HashMap<String, Object> getProperties() {
		return properties;
	}

	public void setProperties(HashMap<String, Object> properties) {
		this.properties = properties;
	}


	void addLabel(String label) {
		labels.add(label);
	}

	public List<String> getLabels() {
		return labels;
	}

	public void setLabels(List<String> labels) {
		this.labels = labels;
	}

	public int getNodeId() {
		if (properties.containsKey("id")) return (int) properties.get("id");
		return -1;
	}

	/**
	 * Check if the node is border or not.
	 * @return true if the node is border.
	 */
	public boolean isBorderNode() {
		for (String label : labels) {
			if (label.equalsIgnoreCase(GenericConstants.BORDER_NODE_LABEL)) {
				return true;
			}
		}

		return false;
	}


	/**
	 * If the node is border, extract the foreign partition that it represents.
	 * @return the partition that the node represents that is stored in the "partition" property.
	 */
	public int getForeignPartitionId() {
		if (properties.containsKey("partition")) return (int) properties.get("partition");
		return -1;
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();

		strBuilder.append("[");
		// Labels
		if (!labels.isEmpty()) {

			for (String label : labels) {
				strBuilder.append(":");
				strBuilder.append(label);
			}
			strBuilder.append("{");
		}

		// Properties
		Iterator propertiesIterator = getProperties().entrySet().iterator();
		while (propertiesIterator.hasNext()) {
			Map.Entry entry = (Map.Entry) propertiesIterator.next();
			strBuilder.append(entry.getKey());
			strBuilder.append(":");
			strBuilder.append(entry.getValue());
			if (propertiesIterator.hasNext()) strBuilder.append(",");
		}
		strBuilder.append("}]");

		return strBuilder.toString();
	}
}
