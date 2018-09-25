package queryStructure;

/**
 * Created by Carla Urrea Blázquez on 07/08/2018.
 *
 * This class represents a pair of key-value (property) related with a variable. This object is useful tu model the
 * SET clause.
 */
public class QSSet extends QSEntity {
	private String property;
	private String newValue;
	private String var;

	public QSSet() {}

	public String getProperty() {
		return property;
	}

	public void setProperty(String property) {
		this.property = property;
	}

	public String getNewValue() {
		return newValue;
	}

	public void setNewValue(String newValue) {
		this.newValue = newValue;
	}

	public String getVar() { return var; }

	public void setVar(String var) { this.var = var; }
}
