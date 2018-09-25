package queryStructure;

import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 27/06/2018.
 *
 * This class models a Ne4j node. This QSNode is part of a QueryStructure. As a node, this has a variable, labels and
 * properties. Also, the property [isRoot] is used to designate if the node, in the QueryStructure, act as root or
 * principal node.
 */
public class QSNode extends QSEntity {
	private  boolean isRoot;
	private String variable;
	private List<String> labels;
	private Map<String, String> properties;

	public QSNode() {
		labels = new ArrayList<>();
		properties = new HashMap<>();
		variable = "";
	}

	String getVariable() {
		return variable;
	}

	public void setVariable(String variable) {
		this.variable = variable;
	}

	boolean isRoot() {
		return isRoot;
	}

	public void setRoot(boolean root) {
		isRoot = root;
	}

	public List<String> getLabels() {
		return labels;
	}

	void setLabels(List<String> labels) {
		this.labels = labels;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	/**
	 * Transforms the object QSNode into a string that follows the format of node's file stored in Hadoop.
	 * @return the string with the node information in hte node's file format.
	 */
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
				strBuilder.append(entry.getValue().replace("\"", ""));
				strBuilder.append("\t");
			}
		}

		return strBuilder.toString();
	}
}
