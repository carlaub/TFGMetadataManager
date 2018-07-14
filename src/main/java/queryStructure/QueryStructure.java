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
	private LinkedHashMap<Type, List<QSEntity>> queryStructure;

	public QueryStructure() {
		queryStructure = new LinkedHashMap<>();
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
	 * Return the root node id from the match clause
	 * @return root node id
	 */
	public int getRootNodeId() {
		if (queryStructure.containsKey(Type.MATCH)) {
			List<QSEntity> list = queryStructure.get(Type.MATCH);

			for (QSEntity entity : list) {
				if (entity instanceof QSNode && ((QSNode) entity).isRoot() && (((QSNode) entity).getProperties().containsKey("id"))) {
					return Integer.valueOf(((QSNode) entity).getProperties().get("id"));
				}
			}
		}

		return -1;
	}


	/**
	 * Convert the query structure to string. Util to execute the Graph Database Service's "execute" function.
	 * @return Query string
	 */
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		List<QSEntity> entityList;

		// MATCH clause
		if (queryStructure.containsKey(Type.MATCH)) {
			entityList = queryStructure.get(Type.MATCH);
			if (!entityList.isEmpty()){
				stringBuilder.append("MATCH ");
				for (QSEntity entity : entityList) {
					if (entity instanceof QSNode) {
						// Node entity
						QSNode node = (QSNode) entity;
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
					} else if (entity instanceof QSRelation){
						// Relationship entity
						QSRelation qsRelation = (QSRelation) entity;
						stringBuilder.append(qsRelation.getStart());
						stringBuilder.append(qsRelation.getType());
						stringBuilder.append(qsRelation.getEnd());
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

	public QueryStructure replaceRootNode (QueryStructure queryStructureOrig, int idRootNodeReplace, ResultNode rootNode) {
		String varRootNode = "";
		QueryStructure queryStructureModified = new QueryStructure();
		Iterator<Map.Entry<Type, List<QSEntity>>> iterator = this.queryStructure.entrySet().iterator();

		while (iterator.hasNext()) {
			Map.Entry<Type, List<QSEntity>> entry = iterator.next();
			Type clauseType = entry.getKey();
			List<QSEntity> entities = entry.getValue();

			if (clauseType == Type.MATCH) {

				for (QSEntity entity : entities) {
					if (entity instanceof QSNode && ((QSNode) entity).isRoot()) {
						QSNode newRootNode = new QSNode();
						newRootNode.isRoot();
						varRootNode = newRootNode.getVariable();
						newRootNode.setProperties(new HashMap<>(((QSNode) entity).getProperties()));

						ArrayList<String> labels = new ArrayList<>();
						labels.add(GenericConstants.BORDER_NODE_LABEL);
						newRootNode.setLabels(labels);

						queryStructureModified.addEntity(clauseType, newRootNode);

					} else {
						queryStructureModified.addEntity(clauseType, entity);
					}
				}
			}

			if (clauseType == Type.WHERE) {
				int index;

				for (QSEntity entity : entities) {
					if (entities instanceof QSCondition) {
						String condition = ((QSCondition) entity).getConditions();
						if ((index = condition.indexOf(varRootNode + ".")) != -1) {
							StringBuilder sbProperty = new StringBuilder();
							char[] conditionCharArray = condition.toCharArray();
							char c;

							// TODO: permitir mas de un "var." en una misma condicion
							for (int i = (index + varRootNode.length() + 1); i < conditionCharArray.length; i++) {
								c = conditionCharArray[i];
								do {
									sbProperty.append(c);
									c = conditionCharArray[i];
								} while (GenericConstants.COMMON_CHARS.indexOf(c) != -1);

								System.out.println("Var en clausula WHERE: " + sbProperty.toString());

								((QSCondition) entity).setCondition(condition.replace((varRootNode + "." + sbProperty.toString()), String.valueOf(rootNode.getProperties().get(sbProperty.toString()))));
							}

						}

						queryStructureModified.addEntity(clauseType, entity);
					}
				}
			}

			if (clauseType == Type.RETURN) {
				for (QSEntity entity : entities) {
					if (entities instanceof QSCondition) {
						String condition = ((QSCondition) entity).getConditions();

						// Root node information has been obtained on the first phase (original query)
						if (!(condition.equals(varRootNode) ||
								condition.matches("^("+ varRootNode +".).*"))) {
							queryStructureModified.addEntity(clauseType, entity);
						}
					}
				}
			}
		}

		System.out.println("-> Query modified: ");
		System.out.println(queryStructureModified.toString());

		return queryStructureModified;
	}
}
