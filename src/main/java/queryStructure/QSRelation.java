package queryStructure;

/**
 * Created by Carla Urrea Blázquez on 27/06/2018.
 */
public class QSRelation extends QSEntity {
	private String variable;
	private String type;
	private String start;
	private String end;

	public QSRelation() {

	}

	public String getVariable() {
		return variable;
	}

	public void setVariable(String variable) {
		this.variable = variable;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
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
}
