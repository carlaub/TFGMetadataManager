package network;

import java.io.Serializable;

/**
 * Created by Carla Urrea Blázquez on 05/05/2018.
 *
 * Msg.java
 */
public class Msg implements Serializable {

	private int code;
	private Object data;

	public Msg(int code, String data) {
		this.code = code;
		this.data = data;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getDataAsString() {
		return data.toString();
	}

	public void setData(String data) {
		this.data = data;
	}

	public Object getData() { return data; }
}
