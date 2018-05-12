package network;

import application.MetadataManager;
import constants.GenericConstants;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

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
	private DatagramSocket dSocket;
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
			dSocket = new DatagramSocket(3456);
		} catch (SocketException e) {
			e.printStackTrace();
		}
		request = new DatagramPacket(buff, buff.length);
	}

	/**
	 * Before start the process, MetadataManager must be connected with all the
	 * SlaveNodes. The numbers of slavesNodes is specified in the configuration file
	 */
	public void waitConnections() {
		int SNConnected = 0;
		int totalSN = MetadataManager.getInstance().getMMInformation().getNumberSlaves();

		// Read requests
		try {
			while (SNConnected < totalSN) {
				dSocket.receive(request);

				byte[] data = request.getData();
				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bais);


				Msg msg = (Msg) ois.readObject();
				System.out.println(request.getAddress() + " - " + request.getPort());
				System.out.println("MSG code: " + msg.getCode() + totalSN);
				System.out.println("MSG data: " + msg.getData());

				SNConnected++;

				MetadataManager.getInstance().addSNConnected(new SlaveNodeObject(SNConnected, request.getPort(), request.getAddress()));
				sendToSlaveNode(SNConnected, GenericConstants.PCK_CODE_ID, String.valueOf(SNConnected));
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
			status = sendToSlaveNode(i + 1, GenericConstants.PCK_CODE_START_DB, "");
			if (!status) return false;
		}

		return true;
	}

	public boolean sendStopDB() {
		int numSN = MetadataManager.getInstance().getMMInformation().getNumberSlaves();
		boolean status;

		for (int i = 0; i < numSN; i++) {
			status = sendToSlaveNode(i + 1, GenericConstants.PCK_DISCONNECT, "");
			if (!status) return false;
		}

		return true;
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
			dSocket.send(dPacketSend);

		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}
}
