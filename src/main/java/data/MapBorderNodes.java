package data;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 27/07/2018.
 *
 */
public class MapBorderNodes {
	private Map<String, Integer> mapBorderNodes;
	public MapBorderNodes() {
		mapBorderNodes = new HashMap<>();
	}

	public void addNewBorderNode(int partFrom, int partTo, int borderNodeID) {
		mapBorderNodes.put(generateKey(partFrom, partTo), borderNodeID);
	}

	public int getBorderNodeID(int partFrom, int partTo) {
		if (contains(partFrom, partTo)) {
			return mapBorderNodes.get(generateKey(partFrom, partTo));
		}

		return -1;
	}

	public boolean contains(int partFrom, int partTo) {
		return mapBorderNodes.containsKey(generateKey(partFrom, partTo));
	}

	private String generateKey(int partFrom, int partTo) {
		return partFrom + "-" + partTo;
	}
}
