package queryStructure;

import constants.GenericConstants;
import neo4j.ResultNode;
import parser.Token;
import parser.Type;

import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 27/06/2018.
 *
 * QueryStructure.java
 */
public class QueryStructure {
	public static final int QUERY_TYPE_DEFAULT = 0;
	public static final int QUERY_TYPE_CREATE = 1;
	public static final int QUERY_TYPE_DELETE = 2;
	public static final int QUERY_TYPE_CHAINED = 3;

	private LinkedHashMap<Type, List<QSEntity>> queryStructure;
	private int queryType;

	public QueryStructure() {
		queryStructure = new LinkedHashMap<>();
		queryType = QUERY_TYPE_DEFAULT;
	}

	public QueryStructure(int queryType) {
		queryStructure = new LinkedHashMap<>();
		queryType = queryType;
	}

	public int getQueryType() {
		return queryType;
	}

	public void setQueryType(int queryType) {
		this.queryType = queryType;
	}

	public void addEntity(Token tClause, QSEntity entity) {
		addEntity(tClause.getType(), entity);
	}

	public void addEntity(Type type, QSEntity entity) {
		if (!queryStructure.containsKey(type)) queryStructure.put(type, new ArrayList<QSEntity>());
		List<QSEntity> list = queryStructure.get(type);
		list.add(entity);
	}

	public List<QSEntity> getList(Token tClause) {
		if (queryStructure.containsKey(tClause)) return queryStructure.get(tClause);
		return null;
	}

	/**
	 * Return true if the query structure has a relation as a
	 * @return
	 */
	public boolean hasRelation() {
		if (queryStructure.containsKey(Type.MATCH)) {
			List<QSEntity> entityList = queryStructure.get(Type.MATCH);
			for (QSEntity entity : entityList) {
				if (entity instanceof QSRelation) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Return the root node of the query string
	 * @return RootNode
	 */
	public QSNode getRootNode() {
		List<QSEntity> list;
		if (queryStructure.containsKey(Type.MATCH)) {
			list = queryStructure.get(Type.MATCH);

		} else if (queryStructure.containsKey(Type.CREATE)) {
			list = queryStructure.get(Type.CREATE);

		} else return null;

		for (QSEntity entity : list) {
			if (entity instanceof QSNode && ((QSNode) entity).isRoot()) {
				return (QSNode) entity;
			}
		}

		return null;
	}

	/**
	 * Return the root node id from the match clause
	 * @return root node id
	 */
	public int getRootNodeId() {
		List<QSEntity> list;

		if (queryStructure.containsKey(Type.MATCH)) {
			list = queryStructure.get(Type.MATCH);

		} else if (queryStructure.containsKey(Type.CREATE)) {
			list = queryStructure.get(Type.CREATE);
			System.out.println("Entra en create!");

		} else return -1;

		for (QSEntity entity : list) {
			if (entity instanceof QSNode && ((QSNode) entity).isRoot() && (((QSNode) entity).getProperties().containsKey("id"))) {
				return Integer.valueOf(((QSNode) entity).getProperties().get("id"));
			}
		}

		return -1;
	}

	/**
	 * Return the [position] QSNode inside the MATCH clause. If there isn't any node at this position, return null.
	 * This function is useful to recover the node's information/ID when a relation should be created.
	 * @return QSNode if exist node in this position, null if not.
	 */
	public QSNode getMatchNodeAt(int position) {
		int currentPosition = 0;

		if (queryStructure.containsKey(Type.MATCH)) {
			List<QSEntity> list = queryStructure.get(Type.MATCH);

			for (QSEntity entity : list) {
				if (entity instanceof QSNode) {
					if (currentPosition == position) return (QSNode) entity;
					currentPosition ++;
				}
			}
		}

		return null;
	}

	/**
	 * Return the QSRelation defined in the CREATE clause. Is a requirement that only one relation be defined in the CREATE clause.
	 * @return The CREATE's relation. If it isn't exists, return null-
	 */
	public QSRelation getCreateRelation() {
		if (queryStructure.containsKey(Type.CREATE)) {
			List<QSEntity> list = queryStructure.get(Type.CREATE);

			for (QSEntity entity : list) {
				if (entity instanceof QSRelation) {
					return (QSRelation) entity;
				}
			}
		}

		return null;
	}

	public int getNodeLevel(String varName) {
		int level = 0;
		List<QSEntity> matchList = queryStructure.get(Type.MATCH);

		for (QSEntity entity : matchList) {
			if (entity instanceof QSNode) {
				if (((QSNode) entity).getVariable().equals(varName)) {
					return level;
				} else {
					level++;
				}
			}
		}

		return -1;
	}

	public boolean hasWhereClauseWithVar() {
		if (queryStructure.containsKey(Type.WHERE)) {
			List<QSEntity> whereList = queryStructure.get(Type.WHERE);

			for (QSEntity entity : whereList) {
				if (entity instanceof QSCondition) {
					if (((QSCondition) entity).getConditions().contains(getRootNode().getVariable() + ".")) return true;
				}
			}
		}

		return false;
	}


	/**
	 * Convert the query structure to string. Util to execute the Graph Database Service's "execute" function.
	 * @return Query string
	 */
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		List<String> secondaryNodeVar = new ArrayList<>();
		List<QSEntity> entityList;


		// MATCH clause
		if (queryStructure.containsKey(Type.MATCH)) {
			boolean previousEntityNode = false;

			entityList = queryStructure.get(Type.MATCH);
			if (!entityList.isEmpty()){
				stringBuilder.append("MATCH ");
				for (QSEntity entity : entityList) {
					if (entity instanceof QSNode) {
						// Node entity
						QSNode node = (QSNode) entity;
						if (previousEntityNode) stringBuilder.append(", ");
						appendNode(node, stringBuilder);

						if (!((QSNode) entity).isRoot()) secondaryNodeVar.add(((QSNode)entity).getVariable());

						previousEntityNode = true;
					} else if (entity instanceof QSRelation){
						// Relationship entity
						QSRelation qsRelation = (QSRelation) entity;
						if (qsRelation.getStart() != null) stringBuilder.append(qsRelation.getStart());
						if (qsRelation.getRelationInfo() != null) stringBuilder.append(qsRelation.getRelationInfo());
						if (qsRelation.getEnd() != null) stringBuilder.append(qsRelation.getEnd());

						previousEntityNode = false;
					}
				}
			}
		}

		// CREATE clause
		if (queryStructure.containsKey(Type.CREATE)) {
			entityList = queryStructure.get(Type.CREATE);
			if (!entityList.isEmpty()) {
				stringBuilder.append("\nCREATE ");

				for (QSEntity entity : entityList) {

					if (entity instanceof QSNode) {
						QSNode node = (QSNode) entity;
						appendNode(node, stringBuilder);

					} else if (entity instanceof QSRelation){
						// TODO Append relation
					}
				}
			}
		}

		// WHERE clause
		if (queryStructure.containsKey(Type.WHERE)) {
			entityList = queryStructure.get(Type.WHERE);
			if (!entityList.isEmpty()) {
				stringBuilder.append("\nWHERE ");
				int size = entityList.size();

				for (int i = 0; i < size - 1; i++) {
					stringBuilder.append(((QSCondition)entityList.get(i)).getConditions());
					stringBuilder.append(", ");
				}

				stringBuilder.append(((QSCondition)entityList.get(size - 1)).getConditions());
			}

			if (!secondaryNodeVar.isEmpty()) {
				// Add extra condition to force the match of border nodes to follow the relation's real path
				for (String var : secondaryNodeVar) {
					stringBuilder.append(" OR ");
					stringBuilder.append(var);
					stringBuilder.append(":");
					stringBuilder.append(GenericConstants.BORDER_NODE_LABEL);
				}
			}
		}

		// RETURN clause
		if (queryStructure.containsKey(Type.RETURN)) {
			entityList = queryStructure.get(Type.RETURN);
			if (!entityList.isEmpty()) {
				stringBuilder.append("\nRETURN ");
				int size = entityList.size();

				for (int i = 0; i < size - 1; i++) {
					stringBuilder.append(((QSCondition)entityList.get(i)).getConditions());
					stringBuilder.append(", ");
				}

				stringBuilder.append(((QSCondition)entityList.get(size - 1)).getConditions());
			}
		}

		return stringBuilder.toString();
	}

	public QueryStructure replaceRootNode (int idRootNodeReplace, ResultNode rootNode, int level) {
		String varRootNode = "";
		QueryStructure queryStructureModified = new QueryStructure();
		Iterator<Map.Entry<Type, List<QSEntity>>> iterator = this.queryStructure.entrySet().iterator();
		boolean rootSet = false;
		List<String> varPrevLevels = new ArrayList<>();

		while (iterator.hasNext()) {
			Map.Entry<Type, List<QSEntity>> entry = iterator.next();
			Type clauseType = entry.getKey();
			List<QSEntity> entities = entry.getValue();

			System.out.println("\n--> LEVEL: " + level);

			if (clauseType == Type.MATCH) {

				for (int i = 0 ; i < (level - 1); i++) {
					// Remove node + relation from previous levels
					// Ej: (n)<--(m)<--(k) [m = level 1] => (m)<--
					varPrevLevels.add(((QSNode)entities.get(0)).getVariable());
					entities.remove(0); // Node
					entities.remove(0); // Relation
				}

				for (QSEntity entity : entities) {
					if (entity instanceof QSNode) {
//						if (((QSNode) entity).isRoot()){
						if (!rootSet) {
							rootSet = true;
							QSNode newRootNode = new QSNode();
							newRootNode.setRoot(((QSNode) entity).isRoot());
							varRootNode = ((QSNode) entity).getVariable();
							newRootNode.setVariable(varRootNode);
							newRootNode.setProperties(new HashMap<>(((QSNode) entity).getProperties()));
							newRootNode.getProperties().put("id", String.valueOf(idRootNodeReplace));

							ArrayList<String> labels = new ArrayList<>();
							labels.add(GenericConstants.BORDER_NODE_LABEL);
							newRootNode.setLabels(labels);

							queryStructureModified.addEntity(clauseType, newRootNode);
						} else {
							queryStructureModified.addEntity(clauseType, entity);
						}
					} else {
						queryStructureModified.addEntity(clauseType, entity);
					}
				}
			}

			if (clauseType == Type.WHERE) {
				int index;

				for (QSEntity entity : entities) {
					if (entity instanceof QSCondition) {
						String condition = ((QSCondition) entity).getConditions();

						while ((index = condition.indexOf(varRootNode + ".")) != -1) {
							System.out.println("--> Variable detectada");
							StringBuilder sbProperty = new StringBuilder();
							char[] conditionCharArray = condition.toCharArray();
							char c;

							// TODO: permitir mas de un "var." en una misma condicion
							int i = (index + varRootNode.length() + 1);
//							for (int i = (index + varRootNode.length() + 1); i < conditionCharArray.length; i++) {
								do {
									c = conditionCharArray[i];
									sbProperty.append(c);
									i++;
								} while (GenericConstants.COMMON_CHARS.indexOf(c) != -1 && i < conditionCharArray.length);

								System.out.println("Var en clausula WHERE: " + sbProperty.toString());

								condition = condition.replace((varRootNode + "." + sbProperty.toString()), String.valueOf(rootNode.getProperties().get(sbProperty.toString())));
//
// 							}

						}

						((QSCondition) entity).setCondition(condition);

						queryStructureModified.addEntity(clauseType, entity);
					}
				}
			}

			if (clauseType == Type.RETURN) {
				for (QSEntity entity : entities) {
					if (entity instanceof QSCondition) {
						String condition = ((QSCondition) entity).getConditions();
						// Root node or prev. levels nodes information has been obtained on the first phase (original query)
						condition = condition.replaceAll("\\s+","");
						if (!(condition.equals(varRootNode) ||
								condition.matches("^("+ varRootNode +".).*") ||
								varPrevLevels.contains(condition))) {
							queryStructureModified.addEntity(clauseType, entity);
						}
					}
				}
			}
		}

		System.out.println("\n\n-> Query modified: ");
		System.out.println(queryStructureModified.toString());

		return queryStructureModified;
	}

	private void appendNode(QSNode node, StringBuilder stringBuilder) {
		// Node entity
		stringBuilder.append("(");
		stringBuilder.append(node.getVariable());

		// Node labels
		if (node.getLabels().size() > 0) {
			List<String> labels = node.getLabels();
			for (String label : labels) {
				stringBuilder.append(":");
				stringBuilder.append(label);
			}
		}

		// Node properties
		if (node.getProperties().size() > 0) {
			Map<String, String> properties = node.getProperties();
			Set<Map.Entry<String, String>> set = properties.entrySet();

			stringBuilder.append("{");

			for (Map.Entry<String, String> entry : set) {
				stringBuilder.append(entry.getKey()+": ");
				stringBuilder.append(entry.getValue() + ",");
			}
			stringBuilder.replace(stringBuilder.length() - 1, stringBuilder.length(), "}" );
		}
		stringBuilder.append(")");
	}
}
