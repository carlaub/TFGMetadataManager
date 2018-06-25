package network;

import application.MetadataManager;
import constants.MsgConstants;
import org.neo4j.graphdb.Result;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 05/05/2018.
 * <p>
 * MMServer.java
 * <p>
 * Manage the communication between MetadadataManager and SlavesNodes
 */
public class MMServer {
	private static MMServer instance;
	private DatagramPacket request;
	private List<DatagramSocket> dSockets;
	byte[] buff;

	public static MMServer getInstance() {
		if (instance == null) {
			instance = new MMServer();
		}
		return instance;
	}

	private MMServer() {
		buff = new byte[65535];
		try {
			dSockets = new ArrayList<>();
			// TODO: Hacer dinámico, no hardcoded
			dSockets.add(0, new DatagramSocket(3456));
			dSockets.add(1, new DatagramSocket(3457));

		} catch (SocketException e) {
			e.printStackTrace();
		}
		request = new DatagramPacket(buff, buff.length);

		// Wait node's connections to the MetadataManager Server
		waitConnections();
	}

	/**
	 * Before start the process, MetadataManager must be connected with all the
	 * SlaveNodes. The numbers of slavesNodes is specified in the configuration file
	 */
	public void waitConnections() {
		int SNConnected = 0;
		int totalSN = MetadataManager.getInstance().getMMInformation().getNumberSlaves();

		System.out.println(MsgConstants.MSG_MMSERVER_WAIT_CON);
		// Read requests
		try {
			while (SNConnected < totalSN) {
				dSockets.get(SNConnected).receive(request);

				byte[] data = request.getData();
				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bais);


				Msg msg = (Msg) ois.readObject();
				System.out.println(request.getAddress() + " - " + request.getPort());
				System.out.println("MSG code: " + msg.getCode() + totalSN);
				System.out.println("MSG data: " + msg.getDataAsString());

				SNConnected++;

				MetadataManager.getInstance().addSNConnected(new SlaveNodeObject(SNConnected, request.getPort(), request.getAddress()));
				sendToSlaveNode(SNConnected, NetworkConstants.PCK_CODE_ID, String.valueOf(SNConnected));
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean sendStartDB() {
		int numSN = MetadataManager.getInstance().getMMInformation().getNumberSlaves();
		boolean status;

		for (int i = 0; i < numSN; i++) {
			status = sendToSlaveNode(i + 1, NetworkConstants.PCK_CODE_START_DB, "");
			if (!status) return false;
		}

		return true;
	}

	public boolean sendStopDB() {
		int numSN = MetadataManager.getInstance().getMMInformation().getNumberSlaves();
		boolean status;

		for (int i = 0; i < numSN; i++) {
			status = sendToSlaveNode(i + 1, NetworkConstants.PCK_DISCONNECT, "");
			if (!status) return false;
		}

		return true;
	}

	/**
	 * Send query to SlaveNode with the specified ID
	 *
	 * @param SNDestId: Slave node ID
	 * @param query:    Query in string format which will be execute in the slave node
	 * @return Neo4J Result obj
	 */
	public void sendQuery(int SNDestId, String query) {
		boolean sent;
		ObjectInputStream ois;
		sent = sendToSlaveNode(SNDestId, NetworkConstants.PCK_QUERY, query);

		if (sent) {
			Msg msg = waitResponseFromSlaveNode(SNDestId);

			if (msg != null && msg.getCode() == NetworkConstants.PCK_QUERY_RESULT) {
				List<Map<String, Object>> result = (List<Map<String, Object>>) msg.getData();

				System.out.println("QUERY RECIBIDA");
				for (Map<String, Object> row : result) {
					System.out.println(row.get("id").toString());
				}
//				return result;
			}
		}

//		return null;
	}


	private boolean sendToSlaveNode(int id, int code, String data) {
		SlaveNodeObject sn = MetadataManager.getInstance().getSNConnected(id);

		if (sn == null) return false;

		try {
			Msg msg = new Msg(code, data);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(msg);
			byte[] dataSend = baos.toByteArray();


			DatagramPacket dPacketSend = new DatagramPacket(dataSend, dataSend.length, sn.getIp(), sn.getPort());
			dSockets.get(id - 1).send(dPacketSend);

		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	private Msg waitResponseFromSlaveNode(int SNDestId) {
		DatagramSocket dSocket = dSockets.get(SNDestId - 1);

		try {
			dSocket.receive(request);
			byte[] data = request.getData();
			ByteArrayInputStream bais = new ByteArrayInputStream(data);

			ObjectInputStream ois = new ObjectInputStream(bais);

			return (Msg) ois.readObject();

		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}

		return null;
	}
}
