package dataStructures;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 27/07/2018.
 *
 * This structure store the borders nodes existing in the systems and the partitions (part.orig - part.dest) that each node
 * connects.
 */
public class MapBorderNodes {
	private Map<String, Integer> mapBorderNodes;

	public MapBorderNodes() {
		mapBorderNodes = new HashMap<>();
	}

	/**
	 * Add new border node to the structure.
	 * @param partFrom origin partition.
	 * @param partTo destination partition.
	 * @param borderNodeID border node's ID.
	 */
	public void addNewBorderNode(int partFrom, int partTo, int borderNodeID) {
		mapBorderNodes.put(generateKey(partFrom, partTo), borderNodeID);
	}

	/**
	 * Get the border node's ID from the partition "from" and the partition "to".
	 * @param partFrom origin partition.
	 * @param partTo destination partition.
	 * @return the borde node's ID.
	 */
	public int getBorderNodeID(int partFrom, int partTo) {
		if (contains(partFrom, partTo)) {
			return mapBorderNodes.get(generateKey(partFrom, partTo));
		}

		return -1;
	}

	/**
	 * Check if a border node exists in the structure.
	 * @param partFrom origin partition.
	 * @param partTo destination partition.
	 * @return true if the border node exists.
	 */
	public boolean contains(int partFrom, int partTo) {
		return mapBorderNodes.containsKey(generateKey(partFrom, partTo));
	}

	/**
	 * Generate a key for a new border note to be inserted. The key is formed by the ID of the origin partition and the
	 * ID of the destination partition separated by "-".
	 * @param partFrom ID of the partition that the border node represents.
	 * @param partTo ID of the parition where the border nodes act as embassy.
	 * @return the border node's key.
	 */
	private String generateKey(int partFrom, int partTo) {
		return partFrom + "-" + partTo;
	}
}
