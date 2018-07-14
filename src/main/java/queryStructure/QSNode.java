package queryStructure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}
