package neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import queryStructure.QueryStructure;

/**
 * Created by Carla Urrea Blázquez on 06/06/2018.
 *
 * QueryExecutor.java
 */
public class QueryExecutor {
	private static QueryExecutor instance;
	private GraphDatabaseService graphDatabaseService;

	public static QueryExecutor getInstace() {
		if (instance == null) instance = new QueryExecutor();
		return instance;
	}

	private QueryExecutor() {
		graphDatabaseService = GraphDatabase.getInstance().getDataBaseGraphService();
	}

	public void processQuery(QueryStructure queryStructure) {
//		try (Transaction q = graphDatabaseService.beginTx();
//			 Result result = graphDatabaseService.execute(queryStructure.toString())) {
//			System.out.println(result.resultAsString());
//
//
//
//			// Important to avoid unwanted behaviour, such as leaking transactions
//			result.close();
//		}
		// If the query hasn't a relation, it must be sent to all partitions

	}

	/**
	 * Process query with relation. If the nodes are related, the "path" must be followed using the border nodes
	 * @param queryStructure
	 */
	private void queryWithRelation(QueryStructure queryStructure) {

	}

	private void queryBroadcast(QueryStructure queryStructure) {

	}
}
