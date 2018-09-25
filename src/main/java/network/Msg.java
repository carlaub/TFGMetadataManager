package network;

import java.io.Serializable;

/**
 * Created by Carla Urrea Blázquez on 05/05/2018.
 *
 * Message structure of the packets send through the network.
 * The code is a numeric frame identifier.
 * The data is the content that wants to be transferred.
 */
public class Msg implements Serializable {

	private int code;
	private Object data;

	Msg(int code, Object data) {
		this.code = code;
		this.data = data;
	}

	int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	String getDataAsString() {
		return data.toString();
	}

	public void setData(String data) {
		this.data = data;
	}

	public Object getData() { return data; }
}