package queryStructure;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Carla Urrea Blázquez on 27/06/2018.
 */
public class QSRelation extends QSEntity {
	private String variable;
	private String relationInfo;
	private String start;
	private String end;
	private String type; //TODO: Soportar mas de un tipo en una misma relacion
	private Map<String, String> properties;

	public QSRelation() {
		properties = new HashMap<>();
		relationInfo = "";
	}

	public String getVariable() {
		return variable;
	}

	public void setVariable(String variable) {
		this.variable = variable;
	}

	public String getRelationInfo() {
		return relationInfo;
	}

//	public void setContent(String relationInfo) {
//		this.relationInfo = relationInfo;
//	}

	public void generateReationInfo() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append('[');
		if (variable != null && !variable.isEmpty()) {
			strBuilder.append(variable);
		}

		if (type != null && !type.isEmpty()) {
			strBuilder.append(':');
			strBuilder.append(type);
		}

		if (properties != null && !properties.isEmpty()) {
			strBuilder.append('{');

			Set<Map.Entry<String, String>> set = properties.entrySet();

			for (Map.Entry<String, String> entry : set) {
				strBuilder.append(entry.getKey());
				strBuilder.append(':');
				strBuilder.append(entry.getValue());
				strBuilder.append(',');
			}
			strBuilder.append("}", strBuilder.length() - 1, strBuilder.length());
		}
		strBuilder.append(']');

		relationInfo = strBuilder.toString();
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	public boolean isRelationLTR() {
		if (end != null && end.contains(">") ||
			start != null && start.contains(">")) {
			return true;
		}

		return false;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void putNewProperty(String key, String value) {
		properties.put(key, value);
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(start);
		strBuilder.append(relationInfo);
		strBuilder.append(end);
		return strBuilder.toString();
	}

	public String toGraphFilesFormat(int idNodeOrg, int idNodeDest) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(String.valueOf(idNodeOrg));
		strBuilder.append("\t");
		strBuilder.append(String.valueOf(idNodeDest));
		strBuilder.append("\t");

		if (type != null && !type.isEmpty()) {
			strBuilder.append(type);
			strBuilder.append("\t");
		}

		if (properties != null && !properties.isEmpty()) {
			Set<Map.Entry<String, String>> set = properties.entrySet();

			for (Map.Entry<String, String> entry : set) {
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
