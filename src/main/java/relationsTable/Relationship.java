package relationsTable;

/**
 * Created by Carla Urrea Blázquez on 21/06/2018.
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
}