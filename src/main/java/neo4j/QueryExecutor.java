package neo4j;

import controllers.QueriesController;
import org.neo4j.graphdb.*;
import queryStructure.QueryStructure;

import java.util.List;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 06/06/2018.
 * <p>
 * QueryExecutor.java
 */
public class QueryExecutor {
	private GraphDatabaseService graphDatabaseService;

	public QueryExecutor() {
		graphDatabaseService = GraphDatabase.getInstance().getDataBaseGraphService();
	}

	public ResultQuery processQuery(QueryStructure query, QueriesController queriesController, boolean trackingMode) {
		ResultQuery resultQuery = processQuery(query.toString());
		queriesController.processQueryResults(resultQuery, query, trackingMode);

		return resultQuery;

	}

	public ResultQuery processQuery(String strQuery) {
		try (Transaction q = graphDatabaseService.beginTx();
			 Result result = graphDatabaseService.execute(strQuery)) {

			System.out.println("Query enviada a particion MM: \n" + strQuery);
			List<String> columnNames = result.columns();
			int columnsCount = columnNames.size();
			ResultQuery resultQuery = new ResultQuery(result.columns());

			while (result.hasNext()) {
				Map<String, Object> next = result.next();

				for (int i = 0; i < columnsCount; i++) {
					Object o = next.get(resultQuery.getColumnsName().get(i));

					if (o instanceof Node) {
						Node node = (Node) o;

						ResultNode resultNode = new ResultNode();

						Iterable<String> properties = node.getPropertyKeys();
						Iterable<Label> labels = node.getLabels();

						for (String propertyKey : properties)
							resultNode.addProperty(propertyKey, node.getProperty(propertyKey));

						for (Label label : labels) resultNode.addLabel(label.name());

						resultQuery.addEntity(i, resultNode);


					} else if (o instanceof Relationship) {
						// Is Relation
						Relationship relationship = (Relationship) o;
						ResultRelation resultRelation = new ResultRelation();

						Iterable<String> properties = relationship.getPropertyKeys();

						for (String propertyKey : properties) {
							resultRelation.addProperty(propertyKey, relationship.getProperty(propertyKey));
						}

						resultRelation.setStartNodeId(relationship.getStartNodeId());
						resultRelation.setEndNodeId(relationship.getEndNodeId());

						resultQuery.addEntity(i, resultRelation);
					}
				}
			}

			q.success();

			// Important to avoid unwanted behaviour, such as leaking transactions
			result.close();

			return resultQuery;
		}

	}

}
