package parser;

import controllers.QueriesController;
import network.MMServer;
import queryStructure.QSCondition;
import queryStructure.QSNode;
import queryStructure.QSRelation;
import queryStructure.QueryStructure;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Carla Urrea Blázquez on 23/06/2018.
 * <p>
 * SyntacticAnalyzer.java
 */
public class SyntacticAnalyzer {
	private static SyntacticAnalyzer instance;
	private LexicographicAnalyzer lex;
	private MMServer mmServer;
	private Token lookahead;
	private List<Type> clausesTypes;

	public static SyntacticAnalyzer getInstance() {
		if (instance == null) instance = new SyntacticAnalyzer();
		return instance;
	}

	private SyntacticAnalyzer() {
		this.lex = LexicographicAnalyzer.getInstance();
		this.mmServer = MMServer.getInstance();


		clausesTypes = Arrays.asList(Type.MATCH, Type.WHERE, Type.RETURN, Type.END);
	}

	public void program() {
		String strQuery = "";
		QueryStructure queryStructure = null;

		lookahead = lex.getToken();


		while (lookahead.getType() != Type.EOF) {
			// New Query
			if (lookahead.getType() == Type.BEGIN) {
				lookahead = lex.getToken();

				System.out.println("Token: " + lookahead.getLexema());

				queryStructure = new QueryStructure();

				while (lookahead.getType() != Type.END) {

					if (lookahead.getType() == Type.MATCH) {
						processClauseMatch(queryStructure, lookahead);
					} else if (lookahead.getType() == Type.WHERE ||
							lookahead.getType() == Type.RETURN) {
						processClauseConditions(queryStructure, lookahead);
						strQuery = strQuery + " " + lookahead.getLexema() + " ";
					} else {
						strQuery = strQuery + lookahead.getLexema();
						lookahead = lex.getToken();
					}
				}
			}

			lookahead = lex.getToken();
			if (queryStructure != null) System.out.println("\nQuery: " + queryStructure.toString() + "\n");

			QueriesController queriesController = new QueriesController();
			queriesController.manageNewQuery(queryStructure);

			//TODO: Logica y tratamiento de la query para saber a que nodo/nodos enviar o cambiar parametro
//			mmServer.sendQuery(1, strQuery);
//			mmServer.sendQuery(2, strQuery);

			strQuery = "";
		}
	}

	public void processClauseMatch(QueryStructure queryStructure, Token clauseToken) {
		boolean parsingLabels = true;
		boolean rootNodeAssigned = false;

		lookahead = lex.getToken();
		// Bucle hasta encontrar la siguiente clausula
		while (!clausesTypes.contains(lookahead.getType())) {

			// NODE CASE
			if (lookahead.getType() == Type.OPAREN) {
				QSNode qsNode = new QSNode();
				// New Node
				lookahead = lex.getToken();
				qsNode.setRoot(!rootNodeAssigned);
				if (!rootNodeAssigned) rootNodeAssigned = true;

				qsNode.setVariable(lookahead.getLexema());

				lookahead = lex.getToken();

				while (lookahead.getType() != Type.CPAREN) {
					switch (lookahead.getType()) {

						case TXT:
							if (parsingLabels) {
								qsNode.getLabels().add(lookahead.getLexema());
							} else {
								// Properties
								String key;
								String value;
								key = lookahead.getLexema();
								lex.getToken(); // Cosume ":"
								lookahead = lex.getToken();
								value = lookahead.getLexema();
								qsNode.getProperties().put(key, value);
							}
							lookahead = lex.getToken();
							break;

						case COMA:
						case CBRACE: // End Properties
						case DOT: // Next is label or property value
						case COLON:
							lookahead = lex.getToken();
							break;

						case OBRACE: // Start properties
							parsingLabels = false;
							lookahead = lex.getToken();

							break;

					}
				}
				lookahead = lex.getToken();
				queryStructure.addEntity(clauseToken, qsNode);
				System.out.println("FIN N");
			}

			// RELATION CASE
			if (lookahead.getType() == Type.DASH ||
					lookahead.getType() == Type.LT) {

				QSRelation qsRelation = new QSRelation();
				String relationStart = lookahead.getLexema();
				lookahead = lex.getToken();

				while ((lookahead.getType() != Type.OPAREN) &&
						(lookahead.getType() != Type.OBRACKET)) {
					relationStart = relationStart + lookahead.getLexema();
					lookahead = lex.getToken();
				}

				System.out.println("START: " + relationStart);
				qsRelation.setStart(relationStart);

				// There is information about the relationship
				if (lookahead.getType() == Type.OBRACKET) {
					String relationInfo = lookahead.getLexema();
					lookahead = lex.getToken();

					while (lookahead.getType() != Type.DASH) {
						relationInfo = relationInfo + lookahead.getLexema();
						lookahead = lex.getToken();
					}
					qsRelation.setType(relationInfo);

					String relationEnd = lookahead.getLexema();
					lookahead = lex.getToken();

					while (lookahead.getType() != Type.OPAREN) {
						relationEnd = relationEnd + lookahead.getLexema();
						lookahead = lex.getToken();
					}
					qsRelation.setEnd(relationEnd);
				}

				queryStructure.addEntity(clauseToken, qsRelation);
				System.out.println("FIN R");
			}
		}
	}

	private void processClauseConditions(QueryStructure queryStructure, Token clauseToken) {
		lookahead = lex.getToken();

		while (!clausesTypes.contains(lookahead.getType())) {
			QSCondition qsCondition = new QSCondition();

			String conditionStr = "";
			while ((lookahead.getType() != Type.COMA) &&
					!clausesTypes.contains(lookahead.getType())) {
				conditionStr = conditionStr + lookahead.getLexema();
				lookahead = lex.getToken();
			}
			qsCondition.setCondition(conditionStr);
			queryStructure.addEntity(clauseToken, qsCondition);

			if (lookahead.getType() == Type.COMA) lookahead = lex.getToken();
		}
	}
}
