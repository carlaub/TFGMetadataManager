package neo4j;

/**
 * Created by Carla Urrea Blázquez on 25/06/2018.
 *
 * ResultRelation.java
 * Class used to return a relation from the query result.
 */
public class ResultRelation extends ResultEntity {

	private long startNodeId;
	private long endNodeId;

	public ResultRelation() {
		super();
	}

	public long getStartNodeId() {
		return startNodeId;
	}

	public void setStartNodeId(long startNodeId) {
		this.startNodeId = startNodeId;
	}

	public long getEndNodeId() {
		return endNodeId;
	}

	public void setEndNodeId(long endNodeId) {
		this.endNodeId = endNodeId;
	}
}