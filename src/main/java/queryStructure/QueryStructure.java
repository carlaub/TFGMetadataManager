package queryStructure;

import parser.Token;
import parser.Type;

import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 27/06/2018.
 *
 * QueryStructure.java
 */
public class QueryStructure {
	LinkedHashMap<Type, List<QSEntity>> queryStructure;

	public QueryStructure() {
		queryStructure = new LinkedHashMap<>();
	}

	public void addEntity(Token tClause, QSEntity entity) {
		if (!queryStructure.containsKey(tClause.getType())) queryStructure.put(tClause.getType(), new ArrayList<QSEntity>());
		List<QSEntity> list = queryStructure.get(tClause.getType());
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
}
