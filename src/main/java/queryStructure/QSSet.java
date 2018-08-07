package queryStructure;

/**
 * Created by Carla Urrea Blázquez on 07/08/2018.
 */
public class QSSet extends QSEntity {
	String property;
	String newValue;

	public QSSet() {}

	public QSSet(String property, String newValue) {
		this.property = property;
		this.newValue = newValue;
	}

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
}
