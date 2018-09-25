package neo4j;

import java.io.Serializable;

/**
 * Created by Carla Urrea Blázquez on 07/08/2018.
 *
 * Represents a result value.
 */
public class ResultValue extends ResultEntity implements Serializable {
	private Object value;

	ResultValue(Object value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return value.toString();
	}
}
