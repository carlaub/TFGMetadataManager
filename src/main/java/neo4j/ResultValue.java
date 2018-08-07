package neo4j;

import java.io.Serializable;

/**
 * Created by Carla Urrea Blázquez on 07/08/2018.
 */
public class ResultValue extends ResultEntity implements Serializable {
	Object value;

	public ResultValue(Object value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return value.toString();
	}
}
