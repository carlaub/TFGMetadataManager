package utils;

import adapter.MetisAdapter;
import application.MetadataManager;
import constants.ErrorConstants;
import controllers.MMController;
import neo4j.GraphDatabase;
import network.MMServer;

import java.util.Scanner;

/**
 * Created by Carla Urrea Blázquez on 22/04/2018.
 *
 * Menu.java
 */

public class Menu {
    private static final String OPT1 = "1.- Export METIS output to Metadata Manager format";
    private static final String OPT2 = "2.- Create the database in the nodes";
    private static final String OPT3 = "3.- Introduce QUERY";
    private static final String OPT4 = "4.- Exit";
    private static final int NUM_OPT = 4;

    private MMController mmController;

    public Menu() {
        this.mmController = new MMController();
    }

    /**
     * Show menu
     */
    public void showMenu() {
        Scanner scan = new Scanner(System.in);

        System.out.println("-- MENU --\n ");
        System.out.println(OPT1);
        System.out.println(OPT2);
        System.out.println(OPT3);
        System.out.println(OPT4);

        System.out.println("\n\nIntroduce option: ");

        processOption(scan.nextLine());

    }

    /**
     * Process the option read. If the option is valid, do the specific routine
     * @param lineRead line read from keyboard
     */
    private void processOption(String lineRead) {
        int opt;

        if (!Checker.checkPositiveNumber(lineRead)) {
            System.out.println(ErrorConstants.ERR_MENU_INPUT_FORMAT);
            showMenu();
            return;
        }


        // Input format OK, go to selected option
        opt = Integer.valueOf(lineRead);

        if (opt > NUM_OPT) {
            // Option out of range
            System.out.println(ErrorConstants.ERR_MENU_OPT_RANGE);
            showMenu();
        }

        // Option OK
        switch(opt) {
            case 1:
                mmController.exportMetisFormat();
                break;
            case 2:
                mmController.createGraphDBInTheNodes();
                break;
            case 3:
                // TODO: Query parser
                break;
            case 4:
                mmController.shutdownSystem();
                break;
            default:
        }

        showMenu();
    }
}
