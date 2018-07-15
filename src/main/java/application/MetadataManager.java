package application;

import network.SlaveNodeObject;
import relationsTable.RelationshipsTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 01/05/2018.
 *
 * MetadataManager.java
 *
 * Singleton class that contains basic information needed by the MetadataManager
 * during the execution
 */
public class MetadataManager {

	private static MetadataManager instance;
	private model.MMInformation MMInformation;
	private ArrayList<SlaveNodeObject> snConnected;

	// ---- Data structures ----
	private static Map<Integer, Integer> mapGraphNodes; // 1 - Structure that holds the relation nodeId -> partition where is located
	private static Map<String, Integer> mapBoarderNodes; // 2 - Each key is composed by [idLocalPartition concat idForeignPartition], the value is the edge node's id
	private static RelationshipsTable relationshipsTable; // 3 - Hash table that contains, for each border node, a list with all the relationships that it has. Node boarder id is the key.

	public MetadataManager() {
		snConnected = new ArrayList<SlaveNodeObject>();
	}

	public static MetadataManager getInstance() {
		if (instance == null) instance = new MetadataManager();

		return instance;
	}

	public model.MMInformation getMMInformation() {
		return MMInformation;
	}

	public void setMMInformation(model.MMInformation MMInformation) {
		this.MMInformation = MMInformation;
	}

	public void addSNConnected(SlaveNodeObject sn) {
		snConnected.add(sn);
	}

	public SlaveNodeObject getSNConnected(int id) {
		if (id > snConnected.size()) return null;
		return snConnected.get(id - 1);
	}

	public Map<Integer, Integer> getMapGraphNodes() {
		if (mapGraphNodes == null) mapGraphNodes = new HashMap<>();
		return mapGraphNodes;
	}

	public Map<String, Integer> getMapBoarderNodes() {
		if (mapBoarderNodes == null) mapBoarderNodes = new HashMap<>();
		return mapBoarderNodes;
	}

	public RelationshipsTable getRelationshipsTable() {
		if (relationshipsTable == null) relationshipsTable = new RelationshipsTable();
		return relationshipsTable;
	}
}
