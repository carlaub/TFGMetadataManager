import constants.MsgConstants;
import controllers.MMController;
import application.MetadataManager;
import constants.ErrorConstants;
import model.MMInformation;
import utils.ParserConf;

import java.io.IOException;

/**
 * Created by Carla Urrea Blázquez on 22/04/2018.
 *
 * Main.java
 */
public class Main {

    public static void main(String[] args) {
        System.out.println("/************* METADATA MANAGER *************/\n\n");

        // Create application singleton object
        MetadataManager metadataManager = MetadataManager.getInstance();

        // Read MetadataManager configuration file
        ParserConf parserConf = new ParserConf();
        try {
            MMInformation mmInformation = parserConf.getConfiguration();
            if (mmInformation != null) {
                metadataManager.setMMInformation(mmInformation);
            } else {
                // ParserError parsing configuration file
                System.out.println(ErrorConstants.ERR_CONF_FILE_PARSER);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        MMController mmController = new MMController();


        mmController.exportMetisFormat();

        mmController.createGraphDBInTheNodes();

        System.out.println(MsgConstants.MSG_PROC_QUERIES);
        mmController.queriesFileExecution();
        System.out.println(MsgConstants.MSG_END_PROC_QUERIES);

        mmController.shutdownSystem();
    }
}
