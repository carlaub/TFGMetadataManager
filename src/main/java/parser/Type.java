package parser;

/**
 * Created by Carla Urrea Blázquez on 23/06/2018.
 *
 * Type.java
 */
public enum Type {
	// Property types
	INTEGER, FLOAT, STRING, BOOLEAN, DATE,

	TXT,

	// Control queries's file reserved keywords to encapsulate each query
	BEGIN, END, EOF,

	// Clauses
	MATCH, WHERE, RETURN,

	// Operators
	OPAREN, CPAREN, OBRACE, CBRACE, COMA, DOT, COLON, OBRACKET, CBRACKET, DASH, LT, GT, QUOTE,
	EQUAL
}
