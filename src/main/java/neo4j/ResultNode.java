package neo4j;

import constants.GenericConstants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Carla Urrea Blázquez on 25/06/2018.
 *
 *
 */
public class ResultNode extends ResultEntity implements Serializable {
	private List<String> labels;

	public ResultNode() {
		super();
		this.labels = new ArrayList<>();
	}



	public void addLabel(String label) {
		labels.add(label);
	}

	public List<String> getLabels() {
		return labels;
	}

	public void setLabels(List<String> labels) {
		this.labels = labels;
	}

	public boolean isBorderNode() {
		for (String label : labels) {
			if (label.equalsIgnoreCase(GenericConstants.BORDER_NODE_LABEL)) {
				return true;
			}
		}

		return false;
	}
}
