import application.MetadataManager;
import constants.ErrorsConstants;
import model.MMInformation;
import utils.Menu;
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

        System.out.println(System.getProperty("user.dir"));
        // Read MetadataManager configuration file
        ParserConf parserConf = new ParserConf();
        try {
            MMInformation mmInformation = parserConf.getConfiguration();
            if (mmInformation != null) {
                metadataManager.setMMInformation(mmInformation);
            } else {
                // Error parsing configuration file
                System.out.println(ErrorsConstants.errConfigurationFileParser);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Menu.showMenu();
    }
}
