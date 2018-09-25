package queryStructure;

import constants.GenericConstants;
import neo4j.ResultNode;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Has;
import parser.Token;
import parser.Type;

import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 27/06/2018.
 * <p>
 * QueryStructure.java
 */
public class QueryStructure {
    public static final int QUERY_TYPE_DEFAULT = 0;
    public static final int QUERY_TYPE_CREATE = 1;
    public static final int QUERY_TYPE_CHAINED = 3;
    public static final int QUERY_TYPE_BROADCAST = 4;
    public static final int QUERY_TYPE_DETACH = 5;
    public static final int QUERY_TYPE_UPDATE = 6;


    private LinkedHashMap<Type, List<QSEntity>> queryStructure;
    private int queryType;

    public QueryStructure() {
        queryStructure = new LinkedHashMap<>();
        queryType = QUERY_TYPE_DEFAULT;
    }

    /**
     * Get the query's type.
     *
     * @return int that represents the type of the query.
     */
    public int getQueryType() {
        return queryType;
    }

    /**
     * Set a query type.
     *
     * @param queryType to be assigned.
     */
    public void setQueryType(int queryType) {
        this.queryType = queryType;
    }

    /**
     * Add new entity to the query structure by the type of the clause.
     *
     * @param tClause token of the clause.
     * @param entity  is the entity to be inserted.
     */
    public void addEntity(Token tClause, QSEntity entity) {
        addEntity(tClause.getType(), entity);
    }

    /**
     * Add new entity to the query structure by the type of the clause.
     *
     * @param type   indicates the type of the clause.
     * @param entity is the entity to be inserted.
     */
    public void addEntity(Type type, QSEntity entity) {
        if (!queryStructure.containsKey(type)) queryStructure.put(type, new ArrayList<QSEntity>());
        List<QSEntity> list = queryStructure.get(type);
        list.add(entity);
    }

    /**
     * Return a list with te entities that are related with the specified clause's type (e.g MATCH, WHERE,...).
     *
     * @param type clause's type.
     * @return a list of entities related with this clause type.
     */
    public List<QSEntity> getList(Type type) {
        if (queryStructure.containsKey(type)) {
            return queryStructure.get(type);
        }

        return null;
    }

    /**
     * Return true if the query structure has a relation as a
     *
     * @return true if the query contains a relationship
     */
    public boolean hasRelation() {
        List<QSEntity> entityList = null;

        if (queryStructure.containsKey(Type.CREATE)) {
            entityList = queryStructure.get(Type.CREATE);
        } else if (queryStructure.containsKey(Type.MATCH)) {
            entityList = queryStructure.get(Type.MATCH);
        }

        if (entityList == null) return false;

        for (QSEntity entity : entityList) {
            if (entity instanceof QSRelation) {
                return true;
            }
        }

        return false;
    }

    /**
     * Return the root node of the query string
     *
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
     *
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
     *
     * @return QSNode if exist node in this position, null if not.
     */
    public QSNode getMatchNodeAt(int position) {
        int currentPosition = 0;

        if (queryStructure.containsKey(Type.MATCH)) {
            List<QSEntity> list = queryStructure.get(Type.MATCH);

            for (QSEntity entity : list) {
                if (entity instanceof QSNode) {
                    if (currentPosition == position) return (QSNode) entity;
                    currentPosition++;
                }
            }
        }

        return null;
    }

    /**
     * Return the QSRelation defined in the CREATE clause. Is a requirement that only one relation be defined in the CREATE clause.
     *
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

    /**************************************************
     * CHAINED QUERY
     *******************************************/

    /**
     * Return the number of variables contained in the MATCH clause.
     *
     * @return the total count of variables in the MATCH clause
     */
    public int getMatchVariablesCount() {
        int count = 0;
        if (queryStructure.containsKey(Type.MATCH)) {
            List<QSEntity> matchList = queryStructure.get(Type.MATCH);

            for (QSEntity entity : matchList) {
                if (entity instanceof QSNode) {
                    count++;
                }
            }
        }

        return count;
    }

    /**
     * Generates a subquery from the original query
     *
     * @param start            represents the index, inside the MATCH clause, of the first variable in the subquery.
     * @param end              represents the index, inside the MATCH clause, of the last variable in the subquery.
     * @param borderStartID    indicates the border ID that substitute the root node.
     * @param idRelForeignNode ID of the root node subtitute. This ID is stored as property in the relation that connects
     *                         root node with border node.
     * @return the QueryStructure of the substring.
     */
    public QueryStructure getSubChainQuery(int start, int end, int borderStartID, int idRelForeignNode) {
        int level = 0;
        boolean rootNodeSet = false;
        boolean nodeAdded = false;
        QueryStructure newQueryStructure = new QueryStructure();

        if (queryStructure.containsKey(Type.MATCH)) {
            List<QSEntity> matchList = queryStructure.get(Type.MATCH);
            List<QSCondition> returnVariables = new ArrayList<>();

            newQueryStructure.setQueryType(QUERY_TYPE_CHAINED);

            if (borderStartID > 0) {
                QSNode borderNodeStart = new QSNode();
                List<String> labels = new ArrayList<>();
                Map<String, String> properties = new HashMap<>();

                labels.add(GenericConstants.BORDER_NODE_LABEL);
                properties.put("id", String.valueOf(borderStartID));

                borderNodeStart.setLabels(labels);
                borderNodeStart.setProperties(properties);
                borderNodeStart.setVariable("b");
                borderNodeStart.setRoot(true);
                rootNodeSet = true;

                newQueryStructure.addEntity(Type.MATCH, borderNodeStart);
            }

            for (QSEntity entity : matchList) {
                if (entity instanceof QSNode) {
                    // NODE
                    if (level >= start && level <= end) {
                        if (!rootNodeSet) {
                            rootNodeSet = true;
                            QSNode nodeRoot = new QSNode();
                            nodeRoot.setRoot(true);
                            nodeRoot.setVariable(((QSNode) entity).getVariable());
                            nodeRoot.setLabels(((QSNode) entity).getLabels());
                            nodeRoot.setProperties(((QSNode) entity).getProperties());


                            newQueryStructure.addEntity(Type.MATCH, nodeRoot);
                        } else {
                            newQueryStructure.addEntity(Type.MATCH, entity);
                        }

                        returnVariables.add(new QSCondition(((QSNode) entity).getVariable()));
                        nodeAdded = true;
                    } else {
                        nodeAdded = false;
                    }

                    level++;
                } else if (entity instanceof QSRelation) {
                    // RELATION
                    if (((level >= start && level <= end) && nodeAdded)) {
                        newQueryStructure.addEntity(Type.MATCH, entity);

                    } else if ((borderStartID > 0) && (level == (start)) && idRelForeignNode >= 0) {
                        QSRelation qsRelation = new QSRelation();

                        qsRelation.setVariable("r");
                        qsRelation.setRelationInfo("[r {idRelForeignNode: \"" + idRelForeignNode + "\" }]");

                        if (((QSRelation) entity).isRelationLTR()) {
                            qsRelation.setStart("-");
                            qsRelation.setEnd("->");
                        } else {
                            qsRelation.setStart("<-");
                            qsRelation.setEnd("-");
                        }

                        newQueryStructure.addEntity(Type.MATCH, qsRelation);
                    }
                }
            }

            for (QSCondition condition : returnVariables) {
                newQueryStructure.addEntity(Type.RETURN, condition);
            }
        }

        return newQueryStructure;
    }

    /**
     * Convert the query structure to string. Util to execute the Graph Database Service's "execute" function.
     *
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
            if (!entityList.isEmpty()) {
                stringBuilder.append("MATCH ");
                for (QSEntity entity : entityList) {
                    if (entity instanceof QSNode) {
                        // Node entity
                        QSNode node = (QSNode) entity;
                        if (previousEntityNode) stringBuilder.append(", ");
                        appendNode(node, stringBuilder);

                        if (!((QSNode) entity).isRoot()) secondaryNodeVar.add(((QSNode) entity).getVariable());

                        previousEntityNode = true;
                    } else if (entity instanceof QSRelation) {
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

                    } else if (entity instanceof QSRelation) {
                        // Relationship entity
                        QSRelation qsRelation = (QSRelation) entity;
                        if (qsRelation.getStart() != null) stringBuilder.append(qsRelation.getStart());
                        if (qsRelation.getRelationInfo() != null) stringBuilder.append(qsRelation.getRelationInfo());
                        if (qsRelation.getEnd() != null) stringBuilder.append(qsRelation.getEnd());
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
                    stringBuilder.append(((QSCondition) entityList.get(i)).getConditions());
                    stringBuilder.append(", ");
                }

                stringBuilder.append(((QSCondition) entityList.get(size - 1)).getConditions());
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

        // SET clause
        if (queryStructure.containsKey(Type.SET)) {
            entityList = queryStructure.get(Type.SET);
            if (!entityList.isEmpty()) {
                stringBuilder.append("\nSET ");
                int size = entityList.size();

                for (int i = 0; i < size - 1; i++) {
                    stringBuilder.append(((QSSet) entityList.get(i)).getVar());
                    stringBuilder.append(".");
                    stringBuilder.append(((QSSet) entityList.get(i)).getProperty());
                    stringBuilder.append(" = ");
                    stringBuilder.append(((QSSet) entityList.get(i)).getNewValue());
                    stringBuilder.append(", ");
                }

                stringBuilder.append(((QSSet) entityList.get(size - 1)).getVar());
                stringBuilder.append(".");
                stringBuilder.append(((QSSet) entityList.get(size - 1)).getProperty());
                stringBuilder.append(" = ");
                stringBuilder.append(((QSSet) entityList.get(size - 1)).getNewValue());
            }
        }

        // RETURN clause
        if (queryStructure.containsKey(Type.RETURN)) {
            entityList = queryStructure.get(Type.RETURN);
            if (!entityList.isEmpty()) {
                stringBuilder.append("\nRETURN ");
                int size = entityList.size();

                for (int i = 0; i < size - 1; i++) {
                    stringBuilder.append(((QSCondition) entityList.get(i)).getConditions());
                    stringBuilder.append(", ");
                }

                stringBuilder.append(((QSCondition) entityList.get(size - 1)).getConditions());
                stringBuilder.append(";");
            }
        }

        // DETACH clause
        if (queryStructure.containsKey(Type.DETACH)) {
            stringBuilder.append("\nDETACH ");
        }

        // DELETE clause
        if (queryStructure.containsKey(Type.DELETE)) {
            entityList = queryStructure.get(Type.DELETE);
            if (!entityList.isEmpty()) {
                stringBuilder.append("DELETE ");
                int size = entityList.size();

                for (int i = 0; i < size - 1; i++) {
                    stringBuilder.append(((QSCondition) entityList.get(i)).getConditions());
                    stringBuilder.append(", ");
                }

                stringBuilder.append(((QSCondition) entityList.get(size - 1)).getConditions());
            }
        }

        return stringBuilder.toString();
    }

    /**
     * Replace the root node inside the query structure.
     *
     * @param idRootNodeReplace ID of the new root node.
     * @param rootNode ResultObject of the rootNode to be replaced. This information is util if the root node is cited in
     *                 the WHERE clause. In this case, is necessary substitute the variable (e.g n.age) for the literal
     *                 value (e.g 24).
     * @param level deep level of the subquery.
     * @return the new QueryStructure that represents the subquery.
     */
    public QueryStructure replaceRootNode(int idRootNodeReplace, ResultNode rootNode, int level) {
        String varRootNode = "";
        QueryStructure queryStructureModified = new QueryStructure();
        Iterator<Map.Entry<Type, List<QSEntity>>> iterator = this.queryStructure.entrySet().iterator();
        boolean rootSet = false;
        List<String> varPrevLevels = new ArrayList<>();

        while (iterator.hasNext()) {
            Map.Entry<Type, List<QSEntity>> entry = iterator.next();
            Type clauseType = entry.getKey();
            List<QSEntity> entities = entry.getValue();


            if (clauseType == Type.MATCH) {

                for (int i = 0; i < (level - 1); i++) {
                    // Remove node + relation from previous levels
                    // Ej: (n)<--(m)<--(k) [m = level 1] => (m)<--
                    varPrevLevels.add(((QSNode) entities.get(0)).getVariable());
                    entities.remove(0); // Node
                    entities.remove(0); // Relation
                }

                for (QSEntity entity : entities) {
                    if (entity instanceof QSNode) {
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
                            StringBuilder sbProperty = new StringBuilder();
                            char[] conditionCharArray = condition.toCharArray();
                            char c;

                            int i = (index + varRootNode.length() + 1);
                            do {
                                c = conditionCharArray[i];
                                sbProperty.append(c);
                                i++;
                            } while (GenericConstants.COMMON_CHARS.indexOf(c) != -1 && i < conditionCharArray.length);


                            condition = condition.replace((varRootNode + "." + sbProperty.toString()), String.valueOf(rootNode.getProperties().get(sbProperty.toString())));
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
                        condition = condition.replaceAll("\\s+", "");
                        if (!(condition.equals(varRootNode) ||
                                condition.matches("^(" + varRootNode + ".).*") ||
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
                stringBuilder.append(entry.getKey() + ": ");
                stringBuilder.append(entry.getValue() + ",");
            }
            stringBuilder.replace(stringBuilder.length() - 1, stringBuilder.length(), "}");
        }
        stringBuilder.append(")");
    }
}
