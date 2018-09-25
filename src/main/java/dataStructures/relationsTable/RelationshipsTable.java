package dataStructures.relationsTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 21/06/2018.
 *
 * Hash structure where, for each key (id border node), there is a list whit all the relations that the boarder node manages
 */

public class RelationshipsTable {

	private Map<Integer, RelationshipsList> relationshipTable;

	public RelationshipsTable() {
		relationshipTable = new HashMap<>();
	}

	/**
	 * Add new relationship in the table. First, this function checks if the key (borderNodeID) exists, if not a new relationshipList
	 * is creates and assigned to the new entrance.
	 * @param idBorderNode border node's ID.
	 * @param relationship the new relationship to insert in the structure.
	 */
	public void addRelation(int idBorderNode, Relationship relationship) {
		RelationshipsList relationshipsList;

		// Check if the boarder node exists in the table
		if (!relationshipTable.containsKey(idBorderNode)) {
			relationshipsList = new RelationshipsList();
			relationshipsList.add(relationship);
			relationshipTable.put(idBorderNode, relationshipsList);
		} else {
			relationshipsList = relationshipTable.get(idBorderNode);
			relationshipsList.add(relationship);
		}
	}

	/**
	 * Check if a relationship exist for the established criteria.
	 * @param borderNodeId border's node ID
	 * @param idNode1 ID of one of the two nodes of the relationship.
	 * @param idNode2 ID of one of the two nodes of the relationship.
	 * @return
	 */
	public boolean existsRelationship(int borderNodeId, int idNode1, int idNode2) {
		RelationshipsList relationshipsList = relationshipTable.get(borderNodeId);

		if (relationshipsList != null) {
			return relationshipsList.containRelation(idNode1, idNode2);
		}

		return false;
	}

	/**
	 * Count the relationships connected with a border node that contains both Node1 and Node2.
	 * @param borderNodeId border node's ID.
	 * @param idNode1 ID of one of the two nodes of the relationship.
	 * @param idNode2 ID of one of the two nodes of the relationship.
	 * @return the count of relations that match the criteria.
	 */
	private int countRelationships (int borderNodeId, int idNode1, int idNode2) {
		RelationshipsList relationshipsList = relationshipTable.get(borderNodeId);

		if (relationshipsList != null) {
			return relationshipsList.countRelations(idNode1, idNode2);
		}

		return -1;
	}

	/**
	 * Get relationships connected to the node with ID = [borderNodeId]
	 * @param borderNodeId border's node ID
	 * @param idNode ID of one of the nodes in the relationship
	 * @return a list of Relationships that match with the search criteria
	 */
	public List<Relationship> getNodeRelationships(int borderNodeId, int idNode) {
		RelationshipsList relationshipsList = relationshipTable.get(borderNodeId);

		if (relationshipsList != null) {
			return relationshipsList.getNodeRelationships(idNode);
		}

		return null;
	}

	/**
	 * Remove the node's relations from the table.
	 * @param borderNodeID border node's ID.
	 */
	public void removeNodeRelations(int borderNodeID) {
		RelationshipsList relationshipsList = relationshipTable.get(borderNodeID);

		if (relationshipsList != null) {
			relationshipsList.removeNodeRelations(borderNodeID);
		}
	}

	/**
	 * Debug function to print all the table information.
	 */
	public void print() {
		for (Map.Entry<Integer, RelationshipsList> entry : relationshipTable.entrySet()) {
			System.out.println("Border node " + entry.getKey());
			entry.getValue().print();
		}
	}
}
