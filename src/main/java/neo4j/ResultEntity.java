package neo4j;

import java.io.Serializable;

/**
 * Created by Carla Urrea Blázquez on 25/06/2018.
 *
 */
public abstract class ResultEntity implements Serializable {
	public abstract String toString();

	public ResultEntity() {}
}
