package neo4j;

import application.MetadataManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;

/**
 * Created by Carla Urrea Blázquez on 29/05/2018.
 *
 * GraphDatabase.java
 *
 * Contains the instance of Neo4j Graph Database Service.
 */
public class GraphDatabase {
	private static GraphDatabase instance = null;
	private GraphDatabaseService graphDatabaseService = null;

	public static GraphDatabase getInstance() {
		if (instance == null) instance = new GraphDatabase();
		return instance;
	}

	private GraphDatabase() {
		this.graphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(MetadataManager.getInstance().getMMInformation().getNeo4jDBPath()));
		if (graphDatabaseService != null) {
			registerShutdownHook(this.graphDatabaseService);
		}
	}

	public GraphDatabaseService getDataBaseGraphService() { return graphDatabaseService; }

	public void shutdown() {
		registerShutdownHook(graphDatabaseService);
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb)  {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				graphDb.shutdown();
			}
		} );
	}
}
