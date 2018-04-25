package adapter;

import constants.GenericConstants;
import model.NodeExportation;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Carla Urrea Blázquez on 22/04/2018.
 * MetisAdapter.java
 *
 * Import the output file of METIS to the format defined and use by BatchInserter with the nodes information and edges.
 * This class need 3 main files to work properly:
 *      -> Output file of METIS: each line corresponds to a number, the value indicates the partition where the node must be located.
 *      -> Nodes file: file that contains, in txt format, all the nodes that form the graph
 *      -> Edges file: file that contains, in txt format, all the relations between the nodes in the graph
 *
 * The output expected after this process is: (Where n == number of graph partitions)
 *      -> n nodes files, one per partition
 *      -> n edges files, one per partition
 */
public class MetisAdapter {

    List<NodeExportation> listGraphNodes;
    String fileNameMetisOutput;
    String fileNameGraphNodes;
    
    public MetisAdapter() {
        listGraphNodes = new ArrayList<NodeExportation>();
    }

    public void beginExport(String fileNameMetisOutput, String fileNameGraphNodes) {
        this.fileNameGraphNodes = fileNameGraphNodes;
        this.fileNameMetisOutput = fileNameMetisOutput;

        processNodeFile();
    }

    private void processNodeFile() {
        BufferedReader readerMetisOutput;
        BufferedReader readerGraphNodes;
        String lineMetisOutput;
        String lineGraphNode;
        int nodeId;
        

        File nodesPart1 = new File(GenericConstants.FILE_NAME_NODES_PARTITION_1);
        File nodesPart2 = new File(GenericConstants.FILE_NAME_NODES_PARTITION_2);
        File nodesPart3 = new File(GenericConstants.FILE_NAME_NODES_PARTITION_3);



        try {
            readerMetisOutput = new BufferedReader(new FileReader(fileNameMetisOutput));
            readerGraphNodes = new BufferedReader(new FileReader(fileNameGraphNodes));


            System.out.println(nodesPart1.getCanonicalPath());
            
            // Read METIS file headerc
            lineMetisOutput = readerMetisOutput.readLine();
                    
            while((lineMetisOutput = readerMetisOutput.readLine()) != null) {
                lineGraphNode = readerGraphNodes.readLine();
                
                if (lineGraphNode != null) {
                    NodeExportation nodeExportation = new NodeExportation();
                    nodeExportation.setId(extractNodeId(lineGraphNode));
                    nodeExportation.setPartitionNumber(extractPartitionNumber(lineMetisOutput));
                    listGraphNodes.add(nodeExportation);

                    System.out.println("New node read: " + nodeExportation.getId() + " - " + nodeExportation.getPartitionNumber());
                    
                } else {
                    //TODO: Show error format files no valid (#lines graph nodes == #lines metis-1)
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*private boolean processEdgeFile() {

    }
*/
    private int extractNodeId(String lineGraphNode) {
        String[] parts = lineGraphNode.split("\\t");
        System.out.println( "PART: " +parts[0]);
        return Integer.valueOf(parts[0]);
    }

    private int extractPartitionNumber(String lineMetisOutput) {
        return Integer.valueOf(lineMetisOutput);
    }
}
