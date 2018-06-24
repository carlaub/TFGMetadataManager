package parser;

/**
 * Created by Carla Urrea Blázquez on 23/06/2018.
 *
 * Token.java
 */
public class Token {

	Type type;
	String lexema;

	public Token(Type type, String lexema) {
		this.type = type;
		this.lexema = lexema;
	}

	public Token(String lexema) {
		this.lexema = lexema;
	}

	public String getLexema() {
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
