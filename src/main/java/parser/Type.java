package parser;

/**
 * Created by Carla Urrea Blázquez on 23/06/2018.
 *
 * Type.java
 */
public enum Type {
	// Property types
	INTEGER, FLOAT, STRING, BOOLEAN, DATE,

	// Control queries's file reserved keywords to encapsulate each query
	BEGIN, END, EOF
}
