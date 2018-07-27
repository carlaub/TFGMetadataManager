package constants;

/**
 * Created by Carla Urrea Blázquez on 22/04/2018.
 *
 * ErrorsConstans.java
 */
public class ErrorConstants {
    // Menu
    public static final String ERR_MENU_INPUT_FORMAT = "[ERROR] Input format invalid";
    public static final String ERR_MENU_OPT_RANGE = "[ERROR] Option invalid";

    // Parser Configuration Fie
    public static final String ERR_CONF_FILE_PARSER = "[ERROR] Unable to parser MetadataManager configuraiton file";

    // Neo4j Import
    public static final String ERR_PARSE_NODE_PARTITION_FILE = "[ERROR] Error reading node partition file from HDFS";
    public static final String ERR_PARSE_EDGE_PARTITION_FILE = "[ERROR] Error reading edge partition file from HDFS";
    public static final String ERR_INIT_BATCHINSERTER = "[ERROR] Error initializing Neo4j Batchinserter";

    // Queries
    public static final String ERR_QUERY_ROOT_NODE_ID = "[ERROR] Query's main node must include its id";
    public static final String ERR_NODE_CREATION = "[ERROR] The node has not been inserted";
    public static final String ERR_RELATION_CREATION = "[ERROR] The relation has not been created";
    public static final String ERR_RELATION_NODE_ID = "[ERROR] Node id not valid";

    // Alterations Manager
    public static final String ERR_UPDATE_METIS_FILE = "[ERROR] Metis file can not be updated";
    public static final String ERR_NODE_ID_DELETE = "[ERROR] Node without ID specified can not be deleted";
}
