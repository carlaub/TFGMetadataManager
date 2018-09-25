package parser;

/**
 * Created by Carla Urrea Blázquez on 23/06/2018.
 *
 * Tokens types.
 */
public enum Type {
	TXT,

	// Control queries's file reserved keywords to encapsulate each query
	BEGIN, END, EOF,

	// Clauses
	MATCH, WHERE, RETURN, CREATE, DELETE, DETACH, SET,

	// Operators
	OPAREN, CPAREN, OBRACE, CBRACE, COMA, DOT, COLON, OBRACKET, CBRACKET, DASH, LT, GT, QUOTE,
	EQUAL
}
