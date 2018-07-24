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

					} else if (lookahead.getType() == Type.CREATE) {
						queryStructure.setQueryType(QueryStructure.QUERY_TYPE_CREATE);
						// CREATE clause are processed as MATCH clauses
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

			strQuery = "";
		}
	}

	public void processClauseMatch(QueryStructure queryStructure, Token clauseToken) {
		boolean parsingLabels = true;
		boolean rootNodeAssigned = false;

		lookahead = lex.getToken();
		// Loop until reach the next clause
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
			}

			// COMA separating nodes
			if (lookahead.getType() == Type.COMA) {
				lookahead = lex.getToken();
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

				String propKey;
				String propValue;

				// There is information about the relationship
				if (lookahead.getType() == Type.OBRACKET) {
					String relationInfo = lookahead.getLexema();
					lookahead = lex.getToken();

					while (lookahead.getType() != Type.CBRACKET) {
						switch (lookahead.getType()) {
							case TXT:
								// Variable
								qsRelation.setVariable(lookahead.getLexema());
								lookahead = lex.getToken();
								break;

							case OBRACE:
								// Properties
								while (lookahead.getType() != Type.CBRACE) {
									lookahead = lex.getToken();

									propKey = lookahead.getLexema();
									lex.getToken(); // :
									lookahead = lex.getToken();
									propValue = lookahead.getLexema();
									qsRelation.putNewProperty(propKey, propValue);
									lex.getToken(); // , o }
								}

								lookahead = lex.getToken();
								break;

							case COLON:
								// Type
								lookahead = lex.getToken();
								if (lookahead.getType() == Type.TXT) {
									qsRelation.setType(lookahead.getLexema());
								}

								lookahead = lex.getToken();
								break;
						}
					}

					lookahead = lex.getToken();
//
//					while (lookahead.getType() != Type.DASH) {
//						relationInfo = relationInfo + lookahead.getLexema();
//						lookahead = lex.getToken();
//					}
//					qsRelation.setContent(relationInfo);

					String relationEnd = lookahead.getLexema();
					lookahead = lex.getToken();

					while (lookahead.getType() != Type.OPAREN) {
						relationEnd = relationEnd + lookahead.getLexema();
						lookahead = lex.getToken();
					}
					qsRelation.setEnd(relationEnd);
					qsRelation.generateReationInfo();
				}

				queryStructure.addEntity(clauseToken, qsRelation);
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
				lookahead = lex.getToken(false);
			}
			qsCondition.setCondition(conditionStr);
			queryStructure.addEntity(clauseToken, qsCondition);

			if (lookahead.getType() == Type.COMA) lookahead = lex.getToken(false);
		}
	}
}
