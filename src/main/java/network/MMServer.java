package network;

import application.MetadataManager;
import constants.MsgConstants;
import controllers.QueriesController;
import neo4j.ResultQuery;
import queryStructure.QueryStructure;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Carla Urrea Blázquez on 05/05/2018.
 *
 * Manage the communication between MetadadataManager and SlavesNodes.
 */
public class MMServer {
	private static MMServer instance;
	private DatagramPacket request;
	private List<DatagramSocket> dSockets;

	public static MMServer getInstance() {
		if (instance == null) {
			instance = new MMServer();
		}
		return instance;
	}

	private MMServer() {

		byte[] buff = new byte[65535];
		try {
			dSockets = new ArrayList<>();
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
	 * SlaveNodes. The numbers of slavesNodes is specified in the configuration file.
	 */
	private void waitConnections() {
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

				//DEBUG
//				Msg msg = (Msg) ois.readObject();
//				System.out.println(request.getAddress() + " - " + request.getPort());
//				System.out.println("MSG code: " + msg.getCode() + totalSN);
//				System.out.println("MSG data: " + msg.getDataAsString());

				SNConnected++;

				MetadataManager.getInstance().addSNConnected(new SlaveNodeObject(SNConnected, request.getPort(), request.getAddress()));
				sendToSlaveNode(SNConnected, NetworkConstants.PCK_CODE_ID, String.valueOf(SNConnected));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Sent the packet to start de DBs instances in each partition.
	 * @return true if the action was successful.
	 */
	public boolean sendStartDB() {
		int numSN = MetadataManager.getInstance().getMMInformation().getNumberSlaves();
		boolean status;
		Msg response;

		for (int i = 0; i < numSN; i++) {
			status = sendToSlaveNode(i + 1, NetworkConstants.PCK_CODE_START_DB, "");
			if (!status) return false;
		}

		for (int i = 0; i < numSN; i++) {
			response = waitResponseFromSlaveNode(i + 1);
			if (response != null) {
				switch (response.getCode()) {
					case NetworkConstants.PCK_STATUS_OK_START_DB:
						break;
					default:
						return false;
				}
			}
		}

		return true;
	}

	/**
	 * Sent the packet through the network to notify all the slave nodes about the system's shutdown.
	 * @return true if the action is executed successfully.
	 */
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
	 */
	public void sendQuery(int SNDestId, QueryStructure query, QueriesController queriesController, boolean trackingMode) {
		boolean sent;
		sent = sendToSlaveNode(SNDestId, NetworkConstants.PCK_QUERY, query.toString());

		if (sent) {
			Msg msg = waitResponseFromSlaveNode(SNDestId);

			if (msg != null && msg.getCode() == NetworkConstants.PCK_QUERY_RESULT) {
				ResultQuery result = (ResultQuery) msg.getData();
				queriesController.processQueryResults(result, query, trackingMode);
			}
		}
	}

	/**
	 * This function send to a SlaveNode a query in String format
	 * @param SNDestId ID of the destination SlaveNode
	 * @param query String that contains the query.
	 * @return the result of the query.
	 */
	public ResultQuery sendStringQuery(int SNDestId, String query) {
		boolean sent;
		sent = sendToSlaveNode(SNDestId, NetworkConstants.PCK_QUERY, query);

		if (sent) {
			Msg msg = waitResponseFromSlaveNode(SNDestId);

			if (msg != null && msg.getCode() == NetworkConstants.PCK_QUERY_RESULT) {
				return (ResultQuery) msg.getData();
			}
		}

		return null;
	}

	/**
	 * Send query to all nodes. Queries without relation in the match clause.
	 * @param queryStructure structure that contains que query's entities in the internal format of the application.
	 * @param queriesController the controller of the queryStructure.
	 */
	public void sendQueryBroadcast(QueryStructure queryStructure, QueriesController queriesController) {
		int numSN = MetadataManager.getInstance().getMMInformation().getNumberSlaves();

		for (int i = 0; i < numSN; i++) {
			sendQuery(i + 1, queryStructure, queriesController, false);
		}
	}

	/**
	 * Sent data to an specific SlaveNode.
	 * @param id of the SlaveNode.
	 * @param code of the packet to send.
	 * @param data of the packet to send.
	 * @return true if the packet was send successfully.
	 */
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

	/**
	 * This function manage the socket waiting for the query response from the SlaveNode.
	 * @param SNDestId ID of the SlaveNode.
	 * @return the message with the response from the SlaveNode.
	 */
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
