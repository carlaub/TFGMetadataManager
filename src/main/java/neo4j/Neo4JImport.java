package neo4j;


import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import constants.MsgConstants;
import org.neo4j.graphdb.*;
import utils.HadoopUtils;

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
	Map<Integer, Node> nodeCache;

	public Neo4JImport() {
		nodeCache = new HashMap<>();
	}

	public boolean startPartitionDBImport() {
		// Init Neo4j batchinserter
		if (!getNeo4jDB()) {
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

		System.out.println(MsgConstants.MSG_FIN_IMPORT_NEO4J);

		return true;
	}

	private boolean getNeo4jDB() {
		graphDb = GraphDatabase.getInstance().getDataBaseGraphService();
		return graphDb != null;
	}

	/**
	 * Node line format:
	 * [id	labelsNum	label1	labelN	Attribute1Name	Attribute1Value	AttributeNName	AttributeNValue]
	 * @param line
	 * @return
	 */
	private void createNode(String line) {
		int labelsNum;
		int index = 0;
		int id;
		Label[] labels;

		String[] parts = line.split("\t");

		// DEBUG
		for(String part : parts) {
			System.out.println("PART: " + part);
		}

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

		// TODO: CAMBIAR ID, UTILIZAR INDEX MANAGER  index() (https://neo4j.com/docs/java-reference/current/javadocs/org/neo4j/graphdb/GraphDatabaseService.html#createNode--)
		// Attributes processing
		while(index < totalParts) {
			properties.put(parts[index], parts[index + 1]);
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

	private boolean processNodesPartitionFile() {
		String line;
		BufferedReader br = HadoopUtils.getInstance().getBufferReaderHFDSFile(MetadataManager.getInstance().getMMInformation().getHDFSWorkingDirectory() + GenericConstants.FILE_NAME_NODES_PARTITION_BASE + GenericConstants.SLAVE_NODE_ID + ".txt");

		if (br == null) return false;

		System.out.println(MsgConstants.MSG_READING_FILE + ": " + GenericConstants.FILE_NAME_NODES_PARTITION_BASE + GenericConstants.SLAVE_NODE_ID + "\n\n");
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
		int fromNode;
		int toNode;

		Map<String, Object> properties = new HashMap<>();

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

		System.out.println(MsgConstants.MSG_READING_FILE + ": " + GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + GenericConstants.SLAVE_NODE_ID + "\n\n");
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
}
