package parser;

import application.MetadataManager;
import constants.GenericConstants;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Scanner;

/**
 * Created by Carla Urrea Blázquez on 23/06/2018.
 * <p>
 * LexicographicAnalyzer.java
 */
public class LexicographicAnalyzer {

	private static LexicographicAnalyzer instance;
	private Scanner scnQueries;
	private String line;
	private int nChar;

	public static LexicographicAnalyzer getInstance() {
		if (instance == null) instance = new LexicographicAnalyzer();
		return instance;
	}

	private LexicographicAnalyzer() {
		nChar = 0;

		try {
			scnQueries = new Scanner(new FileReader(System.getProperty("user.dir") + "/src/main/resources/files/" + MetadataManager.getInstance().getMMInformation().getQueriesFile()));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		if (scnQueries.hasNext()) {
			do {
				line = scnQueries.nextLine();
				line = line + '\n';
			} while (line.charAt(0) == '#' && scnQueries.hasNext());
		} else {
			line = "\n";
		}
	}

	private Token nextToken(boolean ignoreSpaces) {
		char character;
		String lexema = "";
		int state = 0;

		while (true) {
			character = line.charAt(nChar);
			switch (state) {
				case 0:
					if ((character == ' ' && ignoreSpaces) || character == '\t') {
						state = 0;
						nChar++;
					} else if (character == '\n') {
						nChar = 0;
						if (scnQueries.hasNext()) {
							do {
								line = scnQueries.nextLine();
								line = line + '\n';
							} while (line.charAt(0) == '#' && scnQueries.hasNext());
						} else {
							return new Token(Type.EOF, "EOF");
						}
						state = 0;

					} else if ((GenericConstants.COMMON_CHARS.indexOf(character) != -1) || (character == ' ' && !ignoreSpaces)) {
						state = 1;
					} else if ("()[]{}.,:-<>=".indexOf(character) != -1) {
						state = 2;
					} else {
						// Unknown character
						ParserError.showCharError(character);

					}
					break;

				case 1:
					do {

						if ((character == '\'') || (character == '\"')) {
							// Word/sentence between quotes ["] or [']
							do {
								lexema = lexema + character;
								nChar++;
								character = line.charAt(nChar);
							} while((character != '\'') && (character != '\"'));
							character = line.charAt(nChar);
						}

						lexema = lexema + character;
						nChar++;
						character = line.charAt(nChar);



					} while (GenericConstants.COMMON_CHARS.indexOf(character) != -1);

					if (lexema.equalsIgnoreCase("BEGIN")) {
						return new Token(Type.BEGIN, lexema);
					} else if (lexema.equalsIgnoreCase("END")) {
						return new Token(Type.END, lexema);
					} else if (lexema.equalsIgnoreCase("MATCH")) {
						return new Token(Type.MATCH, lexema);
					} else if (lexema.equalsIgnoreCase("WHERE")) {
						return new Token(Type.WHERE, lexema);
					} else if (lexema.equalsIgnoreCase("RETURN")) {
						return new Token(Type.RETURN, lexema);
					} else if (lexema.equalsIgnoreCase("CREATE")) {
						return new Token(Type.CREATE, lexema);
					} else if (lexema.equalsIgnoreCase("DELETE")) {
						return new Token(Type.DELETE, lexema);
					} else if (lexema.equalsIgnoreCase("DETACH")) {
						return new Token(Type.DETACH, lexema);
					} else if (lexema.equalsIgnoreCase("SET")) {
						return new Token(Type.SET, lexema);
					} else if (lexema.equals("-")) {
						return new Token(Type.DASH, lexema);
					} else if (lexema.equals("<")) {
						return new Token(Type.LT, lexema);
					} else if (lexema.equals(">")) {
						return new Token(Type.GT, lexema);
					}

					return new Token(Type.TXT, lexema);

				case 2:
					lexema = lexema + character;
					nChar ++;
					if (character == '(') {
						return new Token(Type.OPAREN, lexema);
					} else if (character == ')') {
						return new Token(Type.CPAREN, lexema);
					} else if (character == '[') {
						return new Token(Type.OBRACKET, lexema);
					} else if (character == ']') {
						return new Token(Type.CBRACKET, lexema);
					} else if (character == '{') {
						return new Token(Type.OBRACE, lexema);
					} else if (character == '}') {
						return new Token(Type.CBRACE, lexema);
					} else if (character == '.') {
						return new Token(Type.DOT, lexema);
					} else if (character == ',') {
						return new Token(Type.COMA, lexema);
					} else if (character == ':') {
						return new Token(Type.COLON, lexema);
					} else if (character == '-') {
						return new Token(Type.DASH, lexema);
					} else if (character == '>') {
						return new Token(Type.GT, lexema);
					} else if (character == '<') {
						return new Token(Type.LT, lexema);
					} else if (character == '=') {
						return new Token(Type.EQUAL, lexema);
					}
			}
		}
	}

	public Token getToken() {
		return getToken(true);
	}

	public Token getToken(boolean ignoreSpaces) {
		Token token = nextToken(ignoreSpaces);
//		System.out.println("Token: " + token.getLexema());
		return token;
	}


	public void close() {
		scnQueries.close();
	}
}
