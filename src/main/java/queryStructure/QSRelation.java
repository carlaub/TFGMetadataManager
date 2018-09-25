package queryStructure;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Carla Urrea Blázquez on 27/06/2018.
 *
 * This class represents a relationships inside the QueryStructure. As a relationship, this object contains the basic
 * information as origin/end node, type, properties, etc.
 */
public class QSRelation extends QSEntity {
	private String variable;
	private String relationInfo;
	private String start;
	private String end;
	private String type;
	private Map<String, String> properties;

	public QSRelation() {
		properties = new HashMap<>();
		relationInfo = "";
	}

	/**
	 * @return the relation variable stored.
	 */
	public String getVariable() {
		return variable;
	}

	/**
	 * Stores the name of the varianble in the relationship.
	 * @param variable string that will be saved.
	 */
	public void setVariable(String variable) {
		this.variable = variable;
	}

	/**
	 * @return the relationship information.
	 */
	public String getRelationInfo() {
		return relationInfo;
	}

	/**
	 * This function generate, in Cypher format, the structure that represents the relationship information and characteristics.
	 * The result is useful to reformat the query.
	 */
	public void generateRelationInfo() {
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

	/**
	 * @return the start of the relationship.
	 */
	String getStart() {
		return start;
	}

	/**
	 * Set the relation's start.
	 * @param start string that contains the characters that forms the relationship start.
	 */
	public void setStart(String start) {
		this.start = start;
	}

	/**
	 * @return the end of the relationship.
	 */
	String getEnd() {
		return end;
	}

	/**
	 * Set the relation's end.
	 * @param end string that contains the characters that forms the relationship end.
	 */
	public void setEnd(String end) {
		this.end = end;
	}

	/**
	 * Check if the relationship is Left To Right.
	 * @return
	 */
	public boolean isRelationLTR() {
		return end != null && end.contains(">") ||
				start != null && start.contains(">");
	}

	/**
	 * @return the relationship type.
	 */
	public String getType() {
		return type;
	}

	/**
	 * Set the relationship type.
	 * @param type of the relationship.
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * Get the map that contains the relationship properties.
	 * @return a Map with the properties represented as a key-value pairs.
	 */
	public Map<String, String> getProperties() {
		return properties;
	}

	/**
	 * Insert a new property (key-value) in the map.
	 * @param key property's name.
	 * @param value propert'y value.
	 */
	public void putNewProperty(String key, String value) {
		properties.put(key, value);
	}

	/**
	 * Set the property's structure.
	 * @param properties the map to be referencied as the relationship properties.
	 */
	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	/**
	 * Set the relationship info.
	 * @param relationInfo string that contains the relationship information.
	 */
	void setRelationInfo(String relationInfo) {
		this.relationInfo = relationInfo;
	}

	/**
	 * Convert the relationship to string adding the relation's start, information and end yo. The final result is a
	 * string that represents the relationship structure in Cypher (e.g. -[r:FRIEND{since: 1996}]->).
	 * @return the relationship expressed in Cypher language.
	 */
	public String toString() {
		return start +
				relationInfo +
				end;
	}

	/**
	 * This function transforms the relationship's information into the format that is use to express the relationship in
	 * the Hadoop files.
	 * @param idNodeOrg ID of the origin node.
	 * @param idNodeDest ID of the destination node.
	 * @return the string with the relation's information expressed in Hadoop file format.
	 */
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

		return strBuilder.toString();
	}
}
