package controllers;

import adapter.MetisAdapter;
import application.MetadataManager;
import neo4j.GraphDatabase;
import neo4j.Neo4JImport;
import neo4j.QueryExecutor;
import network.MMServer;
import parser.SyntacticAnalyzer;
import utils.HadoopUtils;

/**
 * Created by Carla Urrea Blázquez on 04/06/2018.
 *
 * MMController.java
 */
public class MMController {

	private MMServer mmServer;

	public MMController() {
		this.mmServer = MMServer.getInstance();
	}

	// MAIN FEATURES Metadata Manager

	// TODO: Ejecutar el comando de METIS directamente desde el codigo java
	/**
	 * Transform the output from METIS command into the format that the node MetadataManager
	 * is able to parse to create each Neo4j node or relationship.
	 *
	 * The files required:
	 * 	1. File generated by METIS
	 * 	2. File with each node's information/properties
	 * 	3. File with each relationship's information/properties
	 * All the files required and generated will be located into the path provided in the configuration file.
	 */
	public void exportMetisFormat() {
		MetisAdapter metisAdapter = new MetisAdapter();
		// TODO: No tener pre-generado el output de METIS en el proyecto
		metisAdapter.beginExport(System.getProperty("user.dir") + "/src/main/resources/files/graph_example.txt.part.3",
				MetadataManager.getInstance().getMMInformation().getNumberPartitions());

		// TODO: Borrar debug
		MetadataManager.getInstance().getRelationshipsTable().print();

	}

	/**
	 * Create the Neo4j DB with the node partition in each node (include the MetadataManager)
	 */
	public void createGraphDBInTheNodes() {
		// MetadataManager also holds a graph partition
		Neo4JImport neo4JImport = new Neo4JImport();
		neo4JImport.startPartitionDBImport();

		// Send to SlaveNode the order to start the import and create the graph partition based on the files located in
		// Hadoop
		mmServer.sendStartDB();
	}

	public void queriesFileExecution() {
		SyntacticAnalyzer.getInstance().program();
	}

	/**
	 * Exit function. Do here all the task that must be done before shut down the system,
	 */
	public void shutdownSystem() {
		mmServer.sendStopDB();
		HadoopUtils.getInstance().closeResources();
		GraphDatabase.getInstance().shutdown();
		System.exit(0);
	}

	/**
	 *
	 */
	public void executeQuery(String query) {
		QueryExecutor.getInstace().processQuery(query);
	}
}
