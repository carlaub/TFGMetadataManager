package parser;

/**
 * Created by Carla Urrea Blázquez on 23/06/2018.
 *
 * Token is the minimum unit of processing of a original query string. When a query is processed, the token set is analyzed
 * and stored in custom structures designed for this application.
 */
public class Token {

	private Type type;
	private String lexema;

	Token(Type type, String lexema) {
		this.type = type;
		this.lexema = lexema;
	}

	public Token(String lexema) {
		this.lexema = lexema;
	}

	String getLexema() {
		return lexema;
	}

	public void setLexema(String lexema) {
		this.lexema = lexema;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}
}
