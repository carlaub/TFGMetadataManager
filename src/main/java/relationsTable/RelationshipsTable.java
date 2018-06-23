package relationsTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 21/06/2018.
 *
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

	public void print() {
		for (Map.Entry<Integer, RelationshipsList> entry : relationshipTable.entrySet()) {
			System.out.println("Border node " + entry.getKey());
			entry.getValue().print();
		}
	}
}
