package neo4j;


import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import constants.MsgConstants;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;
import hadoop.HadoopUtils;

import java.io.BufferedReader;
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
	private Map<Integer, Node> nodeCache;

	public Neo4JImport() {
		nodeCache = new HashMap<>();
	}

	/**
	 * Start the import from the Hadoop partition's files to the instance hosted in the machine.
	 */
	public void startPartitionDBImport() {
		// Init Neo4j batchinserter
		if (!getNeo4jDB()) {
			System.out.println(ErrorConstants.ERR_INIT_BATCHINSERTER);
		}

		// Reset the Data Base information (Delete all nodes and relations)
		try ( Transaction tx = graphDb.beginTx() ) {
			graphDb.execute("MATCH (n) DETACH DELETE n");
			System.out.println("-> DB Reset done");
			tx.success();
		}

		// Process partition file of nodes
		if (!processNodesPartitionFile()) {
			System.out.println(ErrorConstants.ERR_PARSE_NODE_PARTITION_FILE);
			return;
		}

		// Process partition file of edges
		if (!processEdgesPartitionFile()) {
			System.out.println(ErrorConstants.ERR_PARSE_NODE_PARTITION_FILE);
			return;
		}

		System.out.println(MsgConstants.MSG_FIN_IMPORT_NEO4J);

	}

	/**
	 * This function obtains the Database Graph Service.
	 * @return true if the Database Graph Service has been obtained successfully.
	 */
	private boolean getNeo4jDB() {
		graphDb = GraphDatabase.getInstance().getDataBaseGraphService();
		return graphDb != null;
	}

	/**
	 * Node line format:
	 * [id	labelsNum	label1	labelN	Attribute1Name	Attribute1Value	AttributeNName	AttributeNValue]
	 * @param line is a string with the node information.
	 */
	private void createNode(String line) {
		int labelsNum;
		int index = 0;
		int id;
		Label[] labels;

		String[] parts = line.split("\t");

		int totalParts = parts.length;

		Map<String, Object> properties = new HashMap<>();
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

		// Attributes processing
		String property;
		String value;
		while(index < totalParts) {
			property = parts[index];
			value = parts[index + 1];

			if (StringUtils.isNumeric(value)) {
				properties.put(property, Integer.valueOf(value));
			} else {
				properties.put(property, value);
			}
			index += 2;
		}

		Node n;

		try ( Transaction tx = graphDb.beginTx() ) {
			// Database operations go here
			n = graphDb.createNode(labels);

			for (Map.Entry<String, Object> entry : properties.entrySet()) {
				n.setProperty(entry.getKey(), entry.getValue());
			}
			nodeCache.put(id, n);

			tx.success();
		}
	}

	/**
	 * Analyze and process the node's file.
	 * @return true if the file has been processed successfully.
	 */
	private boolean processNodesPartitionFile() {
		String line;
		BufferedReader br = HadoopUtils.getInstance().getBufferReaderHFDSFile(MetadataManager.getInstance().getMMInformation().getHDFSWorkingDirectory() + GenericConstants.FILE_NAME_NODES_PARTITION_BASE + GenericConstants.MM_SLAVE_NODE_ID + ".txt");

		if (br == null) return false;

		try {
			while((line = br.readLine()) != null) {
				createNode(line);
			}

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	/**
	 * This function creates a new relation inside the Neo4j's instance.
	 * @param line string that represents the relation information in the Hadoop's file format.
	 */
	private void createRelation(String line) {
		int fromNode;
		int toNode;
		RelationshipType type = null;

		Map<String, Object> properties = new HashMap<>();

		String[] parts = line.split("\t");
		int numParts = parts.length;

		fromNode = Integer.parseInt(parts[0]);
		toNode = Integer.parseInt(parts[1]);

		// Relationship type
		if (numParts >= 3) {
			type = DynamicRelationshipType.withName(parts[2]);
		}

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

	/**
	 * Analyze and process the edges's file.
	 * @return true if the file has been processed successfully.
	 */
	private boolean processEdgesPartitionFile() {
		String line;
		BufferedReader br = HadoopUtils.getInstance().getBufferReaderHFDSFile(MetadataManager.getInstance().getMMInformation().getHDFSWorkingDirectory() + GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + GenericConstants.MM_SLAVE_NODE_ID + ".txt");

		if (br == null) return false;

		try {
			while((line = br.readLine()) != null) {
				createRelation(line);
			}

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}
}
