package network;

/**
 * Created by Carla Urrea Blázquez on 24/06/2018.
 *
 * NetworkConstants.java
 */
public class NetworkConstants {

	// Packet code
	public static final int PCK_CODE_ID = 10; // MM sendById id to SN
	public static final int PCK_CODE_START_DB = 11; // MM sendById to SN the order of mount the partition assigned to this node
	public static final int PCK_DISCONNECT = 40; // Disconnect nodes from the system
	public static final int PCK_QUERY = 50; // The packet contains a query
	public static final int PCK_QUERY_RESULT = 51; // The packet contains a query's result
}
