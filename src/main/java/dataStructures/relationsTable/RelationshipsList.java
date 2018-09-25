package dataStructures.relationsTable;

import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 21/06/2018.
 *
 * This class provides a custom list of relations with functions and methods specifically designed for the application.
 */
public class RelationshipsList extends AbstractSequentialList<Relationship> {
	public List<Relationship> list;

	RelationshipsList() {
		list = new LinkedList();
	}

	/**
	 * Add a new relationship.
	 * @param relationship that want to be inserted.
	 * @return true if the relation has been inserted successfully.
	 */
	public boolean add(Relationship relationship) {
		return list.add(relationship);
	}

	/**
	 * Get a relation by its node origin ID.
	 * @param idNodeOrg origin node's ID.
	 * @return a list of Relationships that match with the search criteria.
	 */
	List<Relationship> getRelationsByIdNodeOrg(int idNodeOrg) {
		return getRelationsById(idNodeOrg, true);
	}

	public List<Relationship> getRelationsByIdNodeDest(int idNodeDest) {
		return getRelationsById(idNodeDest, false);
	}

	private List<Relationship> getRelationsById(int id, boolean isNodeOrg) {
		List<Relationship> results = new ArrayList<>();

		for (Relationship relationship : list) {
			if ((isNodeOrg ? relationship.getIdNodeOrg() : relationship.getIdNodeDest()) == id) {
				results.add(relationship);
			}
		}

		return results;
	}

	/**
	 * Check if a relation exists in the structure.
	 * @param idNode1 ID of one of the nodes.
	 * @param idNode2 ID of the other node in the relationship.
	 * @return true if the relationship exists in the structure.
	 */
	boolean containRelation(int idNode1, int idNode2) {
		for (Relationship relationship : list) {
			if (relationship.contains(idNode1, idNode2)) return true;
		}

		return false;
	}

	/**
	 *
	 * @param idNode1 ID of one of the nodes.
	 * @param idNode2 ID of the other node in the relationship.
	 * @return the count of relations that matches the criteria.
	 */
	int countRelations(int idNode1, int idNode2) {
		int count = 0;

		for (Relationship relationship : list) {
			if (relationship.contains(idNode1, idNode2)) count++;
		}

		return count;
	}

	/**
	 * Get relationships that have the [idNode] as origin or destination
	 * @param idNode ID of the node
	 * @return a list of relations that match the search criteria
	 */
	List<Relationship> getNodeRelationships(int idNode) {
		List<Relationship> listNodeRelationships = new ArrayList<>();

		for (Relationship relationship : list) {
			if (relationship.containsNode(idNode)) {
				listNodeRelationships.add(relationship);
			}
		}

		return listNodeRelationships;
	}

	/**
	 * Remove all the relationships that contains a node with ID [nodeID] as origin or destination.
	 * @param nodeID the node's ID.
	 */
	void removeNodeRelations(int nodeID) {
		ListIterator<Relationship> listIterator = list.listIterator();
		Relationship relationship;

		while (listIterator.hasNext()) {
			relationship = listIterator.next();

			if ((relationship.getIdNodeDest() == nodeID) ||
					(relationship.getIdNodeOrg() == nodeID)) {
				listIterator.remove();
			}
		}
	}

	/**
	 * Debug function that prints the information off the structure's relations.
	 */
	void print() {
		for (Relationship relationship : list) {
			System.out.println("\tOrigin: " + relationship.getIdNodeOrg() + "  Destination: " + relationship.getIdNodeDest());
		}
	}

	@Override
	public Relationship get(int index) {
		return list.get(index);
	}

	@Override
	public ListIterator<Relationship> listIterator(int index) {
		return null;
	}

	@Override
	public int size() {
		return list.size();
	}
}


