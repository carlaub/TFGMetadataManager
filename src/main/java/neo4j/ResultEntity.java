package neo4j;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by Carla Urrea Blázquez on 25/06/2018.
 *
 * QSEntity.java
 * QSEntity parent class.
 */
public abstract class ResultEntity implements Serializable {
	protected HashMap<String, Object> properties;

	public abstract String toString();

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
