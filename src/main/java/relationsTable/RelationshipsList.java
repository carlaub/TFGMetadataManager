package relationsTable;

import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 21/06/2018.
 *
 * RelationshipsList.java
 */
public class RelationshipsList extends AbstractSequentialList<Relationship> {
	private List<Relationship> list;

	public RelationshipsList() {
		list = new LinkedList();
	}

	public boolean add(Relationship relationship) {
		return list.add(relationship);
	}

	public List<Relationship> getRelationsByIdNodeOrg(int idNodeOrg) {
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

	public boolean containRelation(int idNode1, int idNode2) {
		for (Relationship relationship : list) {
			if (relationship.contains(idNode1, idNode2)) return true;
		}

		return false;
	}

	public void print() {
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


