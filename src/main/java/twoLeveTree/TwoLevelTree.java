package twoLeveTree;


/**
 * Created by Carla Urrea Blázquez on 19/04/2018.
 *
 * Two Level Tree. Data structure to store 2-node relations. Origin nodes are placed at the first level, destination nodes are placed
 * at the second level.
 * All nodes will be connected with other node of the same level using the pointers "prevTLTNode" & "nextTLTNode".
 * The main structure has 2 pointers to be able to address the first level node group:
 *      -> firstTLTNodeEdge: Points to the first parent node (first LVL 1 node)
 *      -> lastTLTNodeEdge: Points to the last parent node (last LVL 1 node)
 *
 * LVL 1 nodes (type TLTNodeEdge who extends TLTNode) also have two additional pointer to address their children:
 *      -> firstChildTLTNode: Points to the first child
 *      -> lastChildTLTNode: Points to the last child
 *
 * Giving 3 edges/relations: A-B, A-C, A-D; A will be the origin node of this relations. B C & D will be the children of A.
 */

public class TwoLevelTree {
    private TLTNodeEdge firstTLTNodeEdge;
    private TLTNodeEdge lastTLTNodeEdge;


    /**
     * Inserter new node on LVL1 (origin nodes)
     * @param id: Node ID
     * @param partition: Partition/Instance where the node is located
     */
    public TLTNodeEdge insertNodeEdge(int id/*, int partition*/) {
        TLTNodeEdge newTLTNodeEdge = null;

        if (firstTLTNodeEdge == null) {
            // Empty tree
            newTLTNodeEdge = new TLTNodeEdge(id/*, partition*/);
            firstTLTNodeEdge = newTLTNodeEdge;
            lastTLTNodeEdge = newTLTNodeEdge;
        } else {
            // Tree with nodes
            newTLTNodeEdge = new TLTNodeEdge(id,/* partition,*/ lastTLTNodeEdge);
            lastTLTNodeEdge.setNextTLTNode(newTLTNodeEdge);
            newTLTNodeEdge.setPrevTLTNode(lastTLTNodeEdge);
            lastTLTNodeEdge = newTLTNodeEdge;
        }

        return newTLTNodeEdge;
    }

    /**
     * Insert new edge, relation, between two nodes. If the edge node from the destination partition doesn't exist, it is created.
     * @param idNodeEdge
     * @param partitionNodeOrigin
     * @param idNodeDestination
     * @param partitionNodeDestination
     * @return
     */
    public boolean insertNodeRelation(int idNodeEdge, int partitionNodeOrigin, int idNodeDestination, int partitionNodeDestination) {
        TLTNodeEdge nodeOrigin;
        TLTNode nodeDestination;

        nodeOrigin = getNodeEdgeById(idNodeEdge);

        if (nodeOrigin == null) {
            // Node not inserted yet
            nodeOrigin = insertNodeEdge(idNodeEdge/*, partitionNodeOrigin*/);
            if (nodeOrigin == null) {
                System.out.println("[ERROR] Node origin" + idNodeEdge + " couldn't be added.");
                return false;
            }

        }

        if (checkEdgeExist(nodeOrigin, idNodeDestination)) {
            // The edge exist, avoid duplicate
            System.out.println("[ERROR] Edge already exists.");
            return false;
        } else {
            // Add node destination
            nodeDestination = new TLTNode(idNodeDestination, /*partitionNodeDestination,*/ lastTLTNodeEdge.getLastChildTLTNode());

            if (nodeOrigin.getLastChildTLTNode() != null) {
                nodeOrigin.getLastChildTLTNode().setNextTLTNode(nodeDestination);
            }
            if (nodeOrigin.getFirstChildTLTNode() == null) nodeOrigin.setFirstChildTLTNode(nodeDestination);

            // Update parent last node
            nodeOrigin.setLastChildTLTNode(nodeDestination);
        }
        return true;
    }

    /**
     * Check if the edge already exists in the structure or not
     * @param nodeOrigin
     * @param idNodeDestination
     * @return
     */
    private boolean checkEdgeExist(TLTNodeEdge nodeOrigin, int idNodeDestination) {
        TLTNode auxNode = nodeOrigin.getFirstChildTLTNode();

        while (auxNode != null) {
            if (auxNode.getId() == idNodeDestination) return true;
            auxNode = auxNode.getNextTLTNode();
        }

        return false;
    }


    /**
     * Get edge information. Return NULL if the edge doesn't exist.
     * @param idNodeOrigin
     * @param idNodeDestination
     * @return
     */
    public TLTNodeRelation getEdgeInformation(int idNodeOrigin, int idNodeDestination) {
        TLTNodeEdge auxParentNode = getNodeEdgeById(idNodeOrigin);
        // Node origin doesnt' exists
        if (auxParentNode == null) {
            System.out.println("[ERROR] Origin node not found");
            return null;
        }

        TLTNode auxChildNode = getNodeDestinationById(auxParentNode, idNodeDestination);

        // Edge object is created
        if (auxChildNode != null) {
            return new TLTNodeRelation(auxParentNode, auxChildNode);
        } else {
            System.out.println("[ERROR] Desitnation node not found");
            return null;
        }
    }

    /**
     * Return origin node (LVL1 node) by ID
     * @param idNodeOrigin origin node's id
     * @return if node was found, return node, else return null
     */
    private TLTNodeEdge getNodeEdgeById(int idNodeOrigin) {
        if (firstTLTNodeEdge == null) return null;

        TLTNodeEdge currentTLTNode = firstTLTNodeEdge;

        do {
            if (currentTLTNode.getId() == idNodeOrigin) {
                // Origin node was found
                return currentTLTNode;
            } else {
                // Id's don't match
                currentTLTNode = (TLTNodeEdge) currentTLTNode.getNextTLTNode();
            }
        } while(currentTLTNode != null);

        // Any node match with idNodeOrigin
        return null;
    }

    /**
     * Return origin node (LVL1 node) by ID
     * @param idNodedestination origin node's id
     * @return if node was found, return node, else return null
     */
    private TLTNode getNodeDestinationById(TLTNodeEdge parentNode, int idNodedestination) {
        if (parentNode == null) return null;

        TLTNode auxNode = parentNode.getFirstChildTLTNode();
        while (auxNode != null) {
            if (auxNode.getId() == idNodedestination) return auxNode;
            auxNode = auxNode.getNextTLTNode();
        }

        return null;
    }

    /**
     * Debug function to print the tree content
     */
    public void printTree() {
        TLTNodeEdge auxParentNode = firstTLTNodeEdge;
        TLTNode auxChildNode;

        while (auxParentNode != null) {
            System.out.println("\nPARENT NODE " + auxParentNode.getId());
            System.out.println("\t\t\t|");

            //Print children
            auxChildNode = auxParentNode.getFirstChildTLTNode();

            if (auxChildNode != null) {
                do {
                    System.out.println("\t\t\t|-> " + auxChildNode.getId());
                    auxChildNode = auxChildNode.getNextTLTNode();
                } while (auxChildNode != null);
            } else {
                System.out.println("\t\t ** EMPTY **");
            }

            auxParentNode = (TLTNodeEdge) auxParentNode.getNextTLTNode();
        }
    }
}