package constants;

/**
 * Created by Carla Urrea Blázquez on 23/04/2018.
 *
 * GenericConstants.java
 */
public class GenericConstants {

    public static final String FILE_NAME_NODES_PARTITION_BASE = "nodes_part_";
    public static final String FILE_NAME_EDGES_PARTITION_BASE = "edges_part_";

    // Packet code
    public static final int PCK_CODE_ID = 10; // MM send id to SN
    public static final int PCK_CODE_START_DB = 11; // MM send to SN the order of mount the partition assigned to this node
    public static final int PCK_DISCONNECT = 40; // Disconnect nodes from the system

    // Metadata Manager Slave Node
    public static final int SLAVE_NODE_ID = 0;
}
