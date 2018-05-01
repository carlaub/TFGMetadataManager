package model;

/**
 * Created by Carla Urrea Blázquez on 25/04/2018.
 *
 * NodeExportation.java
 */
public class NodeExportation {

    private int id;
    private int partitionNumber;

    public NodeExportation(int id, int partitionNumber) {
        this.id = id;
        this.partitionNumber = partitionNumber;
    }

    public NodeExportation() {}

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public void setPartitionNumber(int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }
}
