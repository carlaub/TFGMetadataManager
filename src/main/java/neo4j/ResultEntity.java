package neo4j;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by Carla Urrea Blázquez on 25/06/2018.
 *
 * ResultEntity.java
 * ResultEntity parent class.
 */
public class ResultEntity implements Serializable {
	private HashMap<String, Object> properties;

	public ResultEntity() {
		this.properties = new HashMap<>();
	}

	public void addProperty(String propertyKey, Object value) {
		properties.put(propertyKey, value);
	}

	public HashMap<String, Object> getProperties() {
		return properties;
	}

	public void setProperties(HashMap<String, Object> properties) {
		this.properties = properties;
	}
}
