package application;

/**
 * Created by Carla Urrea Blázquez on 01/05/2018.
 *
 * MetadataManager.java
 *
 * Singleton class that contains basic information needed by the MetadataManager
 * during the execution
 */
public class MetadataManager {

	private static MetadataManager instance;
	private model.MMInformation MMInformation;

	public MetadataManager() {

	}

	public static MetadataManager getInstance() {
		if (instance == null) {
			instance = new MetadataManager();
		}

		return instance;
	}

	public model.MMInformation getMMInformation() {
		return MMInformation;
	}

	public void setMMInformation(model.MMInformation MMInformation) {
		this.MMInformation = MMInformation;
	}

}
