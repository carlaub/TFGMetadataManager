package parser;

import network.MMServer;

/**
 * Created by Carla Urrea Blázquez on 23/06/2018.
 *
 * SyntacticAnalyzer.java
 */
public class SyntacticAnalyzer {
	private static SyntacticAnalyzer instance;
	private LexicographicAnalyzer lex;
	private MMServer mmServer;
	private Token lookahead;

	public static SyntacticAnalyzer getInstance() {
		if (instance == null) instance = new SyntacticAnalyzer();
		return instance;
	}

	private SyntacticAnalyzer() {
		this.lex = LexicographicAnalyzer.getInstance();
		this.mmServer = MMServer.getInstance();
	}

	public void program() {
		String strQuery = "";

		lookahead = lex.getToken();

		while (lookahead.getType() != Type.EOF){
			// New Query
			if (lookahead.getType() == Type.BEGIN) {
				lookahead = lex.getToken();

				while (lookahead.getType() != Type.END) {
					strQuery = strQuery + " " + lookahead.getLexema();
					lookahead = lex.getToken();
				}
			}

			lookahead = lex.getToken();
			System.out.println("Query: " + strQuery);

			//TODO: Logica y tratamiento de la query para saber a que nodo/nodos enviar o cambiar parametro
			mmServer.sendQuery(1, strQuery);
			mmServer.sendQuery(2, strQuery);

			strQuery = "";
		}
	}
}
