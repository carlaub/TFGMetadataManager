package queryStructure;

/**
 * Created by Carla Urrea Blázquez on 27/06/2018.
 *
 */
public class QSCondition extends QSEntity {
	String condition;

	public QSCondition() { }

	public QSCondition(String condition) {
		this.condition = condition;
	}

	public String getConditions() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}
}
