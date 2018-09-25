package queryStructure;

/**
 * Created by Carla Urrea Blázquez on 27/06/2018.
 *
 * This class represents a condition inside the QueryStructure.
 */
public class QSCondition extends QSEntity {
	private String condition;

	public QSCondition() { }

	QSCondition(String condition) {
		this.condition = condition;
	}

	String getConditions() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}
}
