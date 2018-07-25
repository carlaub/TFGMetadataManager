package queryStructure;

import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 27/06/2018.
 * QSNode.java
 */
public class QSNode extends QSEntity {
	private  boolean isRoot;
	private String variable;
	private List<String> labels;
	private Map<String, String> properties;

	public QSNode() {
		labels = new ArrayList<>();
		properties = new HashMap<>();
	}

	public String getVariable() {
		return variable;
	}

	public void setVariable(String variable) {
		this.variable = variable;
	}

	public boolean isRoot() {
		return isRoot;
	}

	public void setRoot(boolean root) {
		isRoot = root;
	}

	public List<String> getLabels() {
		return labels;
	}

	public void setLabels(List<String> labels) {
		this.labels = labels;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}


	public String toGraphFilesFormat() {
		StringBuilder strBuilder = new StringBuilder();
		if (!properties.containsKey("id")) return null;

		// ID
		strBuilder.append(properties.get("id"));
		strBuilder.append("\t");

		// Num labels
		strBuilder.append(labels.size());
		strBuilder.append("\t");

		// Labels
		for (String label : labels) {
			strBuilder.append(label);
			strBuilder.append("\t");
		}

		// Properties
		Set<Map.Entry<String, String>> set = properties.entrySet();

		for (Map.Entry<String, String> entry : set) {
			// Id is the first item of each row
			if (!entry.getKey().equalsIgnoreCase("id")) {
				// Add new property
				strBuilder.append(entry.getKey());
				strBuilder.append("\t");
				strBuilder.append(entry.getValue());
				strBuilder.append("\t");
			}
		}

		System.out.println("\n--> toGraphFilesFormat: " + strBuilder.toString());
		return strBuilder.toString();
	}

}
