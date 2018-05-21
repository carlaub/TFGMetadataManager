package adapter;


import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import utils.HadoopUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 08/05/2018.
 *
 * Neo4JImport.java
 *
 * This class contains the methods needed for import the graph partition into Neo4J.
 * The finality is interpret the partition files stored in HDFS, parse it and create the Neo4J DB for this partition
 */
public class Neo4JImport {
	private GraphDatabaseService graphDb;
	Map<Integer, Node> nodeCache;

	public Neo4JImport() {
		nodeCache = new HashMap<Integer, Node>();
	}

	public boolean startPartitionDBImport() {
		// Init Neo4j batchinserter
		if (!initBatchInserter()) {
			System.out.println(ErrorConstants.ERR_INIT_BATCHINSERTER);
		}

		// Process partition file of nodes
		if (!processNodesPartitionFile()) {
			System.out.println(ErrorConstants.ERR_PARSE_NODE_PARTITION_FILE);
			return false;
		}

		// Process partition file of edges
		if (!processEdgesPartitionFile()) {
			System.out.println(ErrorConstants.ERR_PARSE_NODE_PARTITION_FILE);
			return false;
		}

		graphDb.shutdown();
		System.out.println("FINISH");


		return true;
	}

	private boolean initBatchInserter() {

		//NEO4J BATCHINSERTER Configuration
		//batchInserter = BatchInserters.inserter(new File(MetadataManager.getInstance().getMMInformation().getNeo4jDBPath()));
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(MetadataManager.getInstance().getMMInformation().getNeo4jDBPath() + "test.db"));
		if (graphDb != null) {
			registerShutdownHook(graphDb);
			return true;
		}

		return false;
	}

	/**
	 * Node line format:
	 * [id	labelsNum	label1	labelN	Attribute1Name	Attribute1Value	AttributeNName	AttributeNValue]
	 * @param line
	 * @return
	 */
	private boolean createNode(String line) {
		int labelsNum;
		int index = 0;
		int id;
		long neo4jId;
		Label[] labels;

		String[] parts = line.split("\t");

		// DEBUG
		for(String part : parts) {
			System.out.println("PART: " + part);
		}

		int totalParts = parts.length;

		Map<String, Object> properties = new HashMap<String, Object>();
		id = Integer.parseInt(parts[index]);
		index ++;
		properties.put("id", id);

		// Labels processing
		labelsNum = Integer.parseInt(parts[index]);
		index++;
		labels = new Label[labelsNum];
		for (int i = 0; i < labelsNum; i++) {
			labels[i] = DynamicLabel.label(parts[index]);
			index++;
		}

		// TODO: CAMBIAR ID, UTILIZAR INDEX MANAGER  index() (https://neo4j.com/docs/java-reference/current/javadocs/org/neo4j/graphdb/GraphDatabaseService.html#createNode--)
		// Attributes processing
		while(index < totalParts) {
			properties.put(parts[index], parts[index + 1]);
			index += 2;
		}

//		neo4jId = batchInserter.createNode(properties, labels);
		/*
		System.out.println("-> NODE ID: " + neo4jId);
		if (neo4jId >= 0) {
			nodeCache.put(id, neo4jId);
			return true;
		}*/
		Node n;

		try ( Transaction tx = graphDb.beginTx() ) {
			// Database operations go here
			n = graphDb.createNode(labels);

			for (Map.Entry<String, Object> entry : properties.entrySet()) {
				n.setProperty(entry.getKey(), entry.getValue());
			}
			nodeCache.put(id, n);
			System.out.println("Node created");

			tx.success();
		}


		return false;
	}

	private boolean processNodesPartitionFile() {
		String line;
		BufferedReader br = HadoopUtils.getInstance().getBufferReaderHFDSFile(MetadataManager.getInstance().getMMInformation().getHDFSWorkingDirectory() + GenericConstants.FILE_NAME_NODES_PARTITION_BASE + GenericConstants.SLAVE_NODE_ID + ".txt");

		if (br == null) return false;

		System.out.println("READING FILE: " + GenericConstants.FILE_NAME_NODES_PARTITION_BASE + GenericConstants.SLAVE_NODE_ID + "\n\n");
		try {
			while((line = br.readLine()) != null) {
				System.out.println(line);
				createNode(line);
			}

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	private void createRelation(String line) {
		int index = 0;
		int fromNode;
		int toNode;

		Map<String, Object> properties = new HashMap<String, Object>();

		String[] parts = line.split("\t");
		int numParts = parts.length;

		fromNode = Integer.parseInt(parts[0]);
		toNode = Integer.parseInt(parts[1]);

		// Relationship type
		RelationshipType type = DynamicRelationshipType.withName(parts[2]);

		for (int i = 3; i < numParts; i = i + 2) {
			properties.put(parts[i], parts[i + 1]);
		}

		try ( Transaction tx = graphDb.beginTx() ) {
			// Relationship properties processing
			Relationship relationShip;
			relationShip = nodeCache.get(fromNode).createRelationshipTo(nodeCache.get(toNode), type);

			for (Map.Entry<String, Object> entry : properties.entrySet()) {
				relationShip.setProperty(entry.getKey(), entry.getValue());
			}

			tx.success();
		}
	}

	private boolean processEdgesPartitionFile() {
		String line;
		BufferedReader br = HadoopUtils.getInstance().getBufferReaderHFDSFile(MetadataManager.getInstance().getMMInformation().getHDFSWorkingDirectory() + GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + GenericConstants.SLAVE_NODE_ID + ".txt");

		if (br == null) return false;

		System.out.println("READING FILE: " + GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + GenericConstants.SLAVE_NODE_ID + "\n\n");
		try {
			while((line = br.readLine()) != null) {
				System.out.println(line);
				createRelation(line);
			}

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
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
