package dataStructures.relationsTable;

/**
 * Created by Carla Urrea Blázquez on 27/07/2018.
 *
 * This class model a relationship with a origin and destination node's IDs
 */
public class Relationship {

	private int idNodeOrg;
	private int idNodeDest;

	public Relationship(int idNodeOrg, int idNodeDest) {
		this.idNodeOrg = idNodeOrg;
		this.idNodeDest = idNodeDest;
	}

	public int getIdNodeOrg() {
		return idNodeOrg;
	}

	public void setIdNodeOrg(int idNodeOrg) {
		this.idNodeOrg = idNodeOrg;
	}

	public int getIdNodeDest() {
		return idNodeDest;
	}

	public void setIdNodeDest(int idNodeDest) {
		this.idNodeDest = idNodeDest;
	}

	public boolean contains(int idNode1, int idNode2) {
		return ((idNodeDest == idNode1 && idNodeOrg == idNode2) ||
				(idNodeDest == idNode2 && idNodeOrg == idNode1));
	}

	boolean containsNode(int idNode) {
		return ((idNodeDest == idNode) || (idNodeOrg == idNode));
	}
}