package network;

import java.net.InetAddress;

/**
 * Created by Carla Urrea Blázquez on 06/05/2018.
 *
 * SlaveNodeObject.java
 *
 * This class holds the information that the MetadataManager needs from each SlaveNode
 * connected
 */
public class SlaveNodeObject {
	private int id;
	private int port;
	private InetAddress ip;

	public SlaveNodeObject(int id, int port, InetAddress ip) {
		this.id = id;
		this.port = port;
		this.ip = ip;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public InetAddress getIp() {
		return ip;
	}

	public void setIp(InetAddress ip) {
		this.ip = ip;
	}
}
