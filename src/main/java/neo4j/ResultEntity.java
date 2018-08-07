package neo4j;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by Carla Urrea Blázquez on 25/06/2018.
 *
 */
public abstract class ResultEntity implements Serializable {
	protected HashMap<String, Object> properties;

	public abstract String toString();

	public ResultEntity() {

	}
}
