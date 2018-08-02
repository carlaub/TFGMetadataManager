package relationsTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 21/06/2018.
 * <p>
 * Hash structure where, for each key (id border node), there is a list whit all the relations that the boarder node manages
 */

public class RelationshipsTable {

	Map<Integer, RelationshipsList> relationshipTable;

	public RelationshipsTable() {
		relationshipTable = new HashMap<>();
	}

	public RelationshipsList getRelations(int boarderNodeId) {
		return relationshipTable.get(boarderNodeId);
	}

	public List<Relationship> getRelationsByIdNodeOrg(int boarderNodeId, int idNodeOrg) {
		RelationshipsList relationshipsList = getRelations(boarderNodeId);
		return relationshipsList.getRelationsByIdNodeOrg(idNodeOrg);
	}

	public List<Relationship> getRelationsByIdNodeDest(int boarderNodeId, int idNodeDest) {
		RelationshipsList relationshipsList = getRelations(boarderNodeId);
		return relationshipsList.getRelationsByIdNodeOrg(idNodeDest);
	}

	public void addRelation(int idBoarderNode, Relationship relationship) {
		RelationshipsList relationshipsList;

		// Check if the boarder node exists in the table
		if (!relationshipTable.containsKey(idBoarderNode)) {
			relationshipsList = new RelationshipsList();
			relationshipsList.add(relationship);
			relationshipTable.put(idBoarderNode, relationshipsList);
		} else {
			relationshipsList = relationshipTable.get(idBoarderNode);
			relationshipsList.add(relationship);
		}
	}

	public boolean existsRelationship(int borderNodeId, int idNode1, int idNode2, int fetchedResults) {
		if (existsRelationship(borderNodeId, idNode1, idNode2)) {
			if (countRelationships(borderNodeId, idNode1, idNode2) <= fetchedResults) {
				return true;
			}
		}

		return false;
	}

	public boolean existsRelationship(int borderNodeId, int idNode1, int idNode2) {
		System.out.println("Border node id: " + borderNodeId + " idNode1: " + idNode1 + " idNode2: " + idNode2);
		RelationshipsList relationshipsList = relationshipTable.get(borderNodeId);

		if (relationshipsList != null) {
			return relationshipsList.containRelation(idNode1, idNode2);
		}

		return false;
	}

	public int countRelationships (int borderNodeId, int idNode1, int idNode2) {
		RelationshipsList relationshipsList = relationshipTable.get(borderNodeId);

		if (relationshipsList != null) {
			return relationshipsList.countRelations(idNode1, idNode2);
		}

		return -1;
	}

	public List<Relationship> getNodeRelationships(int borderNodeId, int idNode) {

		System.out.println("Border node id: " + borderNodeId + " idNode: " + idNode);
		RelationshipsList relationshipsList = relationshipTable.get(borderNodeId);

		if (relationshipsList != null) {
			return relationshipsList.getNodeRelationships(idNode);
		}

		return null;
	}

	public void removeNodeRelations(int borderNodeID, int nodeToRemoveID) {
		RelationshipsList relationshipsList = relationshipTable.get(borderNodeID);

		if (relationshipsList != null) {
			relationshipsList.removeNodeRelations(borderNodeID);
		}
	}

	public void print() {
		for (Map.Entry<Integer, RelationshipsList> entry : relationshipTable.entrySet()) {
			System.out.println("Border node " + entry.getKey());
			entry.getValue().print();
		}
	}
}
