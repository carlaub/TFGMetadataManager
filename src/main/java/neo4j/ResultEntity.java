package neo4j;

import java.io.Serializable;

/**
 * Created by Carla Urrea Blázquez on 25/06/2018.
 *
 * Base class of the ResultQuery entities (e.g. Nodes, Relationships, etc.).
 */
public abstract class ResultEntity implements Serializable {
	public abstract String toString();

	ResultEntity() {}
}
