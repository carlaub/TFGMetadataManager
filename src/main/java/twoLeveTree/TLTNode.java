package twoLeveTree;


public class TLTNode {
    private int id;
    private TLTNode prevTLTNode;
    private TLTNode nextTLTNode;
//    private int partition;

    public TLTNode(int id/*, int partition*/) {
        this.id = id;
//        this.partition = partition;
    }

    public TLTNode(int id,/* int partition,*/ TLTNode previousTLTNode) {
        this.id = id;
//        this.partition = partition;
        this.prevTLTNode = previousTLTNode;
        this.nextTLTNode = null;
    }

    public int getId() {
        return id;
    }

    public TLTNode getPrevTLTNode() {
        return prevTLTNode;
    }

    public TLTNode getNextTLTNode() {
        return nextTLTNode;
    }

//    public int getPartition() {
//        return partition;
//    }

    public void setPrevTLTNode(TLTNode prevTLTNode) {
        this.prevTLTNode = prevTLTNode;
    }

    public void setNextTLTNode(TLTNode nextTLTNode) {
        this.nextTLTNode = nextTLTNode;
    }
}
