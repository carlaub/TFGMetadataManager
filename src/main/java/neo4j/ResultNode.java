package neo4j;

import constants.GenericConstants;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 25/06/2018.
 *
 *
 */
public class ResultNode extends ResultEntity implements Serializable {
	private List<String> labels;
	protected HashMap<String, Object> properties;


	public ResultNode() {
		super();
		this.labels = new ArrayList<>();
		this.properties = new HashMap<>();
	}

	public void addProperty(String propertyKey, Object value) {
		properties.put(propertyKey, value);
	}

	public HashMap<String, Object> getProperties() {
		return properties;
	}

	public void setProperties(HashMap<String, Object> properties) {
		this.properties = properties;
	}


	public void addLabel(String label) {
		labels.add(label);
	}

	public List<String> getLabels() {
		return labels;
	}

	public void setLabels(List<String> labels) {
		this.labels = labels;
	}

	public boolean isBorderNode() {
		for (String label : labels) {
			if (label.equalsIgnoreCase(GenericConstants.BORDER_NODE_LABEL)) {
				return true;
			}
		}

		return false;
	}

	public int getNodeId() {
		if (properties.containsKey("id")) return (int)properties.get("id");
		return -1;
	}

	public int getForeignPartitionId() {
		if (properties.containsKey("partition")) return (int)properties.get("partition");
		return -1;
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();

		strBuilder.append("[");
		// Labels
		if (!labels.isEmpty()) {
			int labelsListSize = labels.size();

			for (int i = 0; i < labelsListSize; i++) {
				strBuilder.append(":");
				strBuilder.append(labels.get(i));
			}
			strBuilder.append("{");
		}

		// Properties
		Iterator propertiesIterator = getProperties().entrySet().iterator();
		while(propertiesIterator.hasNext()) {
			Map.Entry entry = (Map.Entry)propertiesIterator.next();
			strBuilder.append(entry.getKey());
			strBuilder.append(":");
			strBuilder.append(entry.getValue());
			if (propertiesIterator.hasNext()) strBuilder.append(",");
		}
		strBuilder.append("}]");


		return strBuilder.toString();
	}
}
