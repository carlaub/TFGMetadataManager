package twoLeveTree;

/**
 * Created by Carla Urrea Blázquez on 19/04/2018.
 */
public class TLTNodeEdge extends TLTNode{
    private TLTNode firstChildTLTNode;
    private TLTNode lastChildTLTNode;


    public TLTNodeEdge(int id/* int partition*/) {
        super(id/*, partition*/);
    }

    public TLTNodeEdge(int id,/* int partition,*/ TLTNode previousTLTNode) {
        super(id,/* partition,*/ previousTLTNode);
    }

    public TLTNode getLastChildTLTNode() {
        return lastChildTLTNode;
    }

    public TLTNode getFirstChildTLTNode() {
        return firstChildTLTNode;
    }

    public void setLastChildTLTNode(TLTNode lastChild) {
        this.lastChildTLTNode = lastChild;
    }

    public void setFirstChildTLTNode(TLTNode firstChild) {
        this.firstChildTLTNode = firstChild;
    }
}
