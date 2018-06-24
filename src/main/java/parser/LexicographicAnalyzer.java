package parser;

import application.MetadataManager;

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
			line = scnQueries.nextLine();
			line = line + '\n';
		} else {
			line = "\n";
		}
	}

	private Token nextToken() {
		char character;
		String lexema = "";
		int state = 0;

		while (true) {
			character = line.charAt(nChar);
			switch (state) {
				case 0:
					if (character == ' ' || character == '\t') {
						state = 0;
						nChar++;
					} else if (character == '\n') {
						nChar = 0;
						if (scnQueries.hasNext()) {
							line = scnQueries.nextLine();
							line = line + '\n';
						} else {
							return new Token(Type.EOF, "EOF");
						}
						state = 0;

//					} else if ("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".indexOf(character) != -1) {
//						state = 1;
//					} else if ("0123456789".indexOf(character) != -1) {
//						state = 2;
//					} else {
//						// Unknown character
//						ParserError.showCharError(character);

					} else {
						state = 1;
					}
					break;

				case 1:
					do {
						lexema = lexema + character;
						nChar++;
						character = line.charAt(nChar);
					} while ("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".indexOf(character) != -1);

					if (lexema.equalsIgnoreCase("BEGIN")) {
						return new Token(Type.BEGIN, lexema);
					} else if (lexema.equalsIgnoreCase("END")) {
						return new Token(Type.END, lexema);

					}
					return new Token(lexema);

				case 2:
					break;
			}
		}

	}

	public Token getToken() {
		Token token = nextToken();
		System.out.println("Token: " + token.getLexema());
		return token;
	}


	public void close() {
		scnQueries.close();
	}
}
