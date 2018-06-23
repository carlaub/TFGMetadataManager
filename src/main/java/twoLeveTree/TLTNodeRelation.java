package twoLeveTree;

/**
 * Created by Carla Urrea Blázquez on 21/04/2018.
 */

public class TLTNodeRelation {
    private TLTNodeEdge nodeEdge;
    private TLTNode nodeDestination;

    public TLTNodeRelation(TLTNodeEdge nodeEdge, TLTNode nodeDestination) {
        this.nodeEdge = nodeEdge;
        this.nodeDestination = nodeDestination;

        System.out.println(nodeEdge.getId() + " (Part. " /*+ nodeEdge.getPartition() */+ ") ------------> "
                            + nodeDestination.getId() + " (Part. " /*+ nodeDestination.getPartition()*/ + ")");
    }

    public TLTNodeEdge getNodeEdge() {
        return nodeEdge;
    }

    public void setNodeEdge(TLTNodeEdge nodeEdge) {
        this.nodeEdge = nodeEdge;
    }

    public TLTNode getNodeDestination() {
        return nodeDestination;
    }

    public void setNodeDestination(TLTNode nodeDestination) {
        this.nodeDestination = nodeDestination;
    }
}
