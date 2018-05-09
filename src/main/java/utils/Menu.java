package utils;

import adapter.MetisAdapter;
import application.MetadataManager;
import constants.ErrorsConstants;
import constants.GenericConstants;
import network.MMServer;

import java.util.Scanner;

/**
 * Created by Carla Urrea Blázquez on 22/04/2018.
 *
 * Menu.java
 */

public class Menu {
    public static final String OPT1 = "1.- Export METIS output to BatchInserter format";
    public static final String OPT2 = "2.- Start DB on nodes";
    public static final String OPT3 = "3.- Introduce QUERY";
    public static final String OPT4 = "4.- Exit";
    public static final int NUM_OPT = 4;

    /**
     * Show menu
     */
    public static void showMenu() {
        Scanner scan = new Scanner(System.in);
        String lineRead;

        System.out.println("-- MENU --\n ");
        System.out.println(OPT1);
        System.out.println(OPT2);
        System.out.println(OPT3);
        System.out.println(OPT4);

        System.out.println("\n\nIntroduce option: ");

        processOption(scan.nextLine());

    }

    /**
     * Process the option readed. If the option is valid, do the specific routine
     * @param lineRead line read from keyboard
     */
    private static void processOption(String lineRead) {
        int opt;

        if (!Checker.checkPositiveNumber(lineRead)) {
            System.out.println(ErrorsConstants.errMenuInputFormat);
            showMenu();
            return;
        }


        // Input format OK, go to selected option
        opt = Integer.valueOf(lineRead);

        if (opt > NUM_OPT) {
            // Option out of range
            System.out.println(ErrorsConstants.errMenuOptRange);
            showMenu();
        }

        // Option OK
        switch(opt) {
            case 1:
                System.out.println("OPT 1 selected");
                MetisAdapter metisAdapter = new MetisAdapter();
                metisAdapter.beginExport(System.getProperty("user.dir") + "/src/main/resources/files/graph_example.txt.part.3",
                        MetadataManager.getInstance().getMMInformation().getNumberPartitions());
                break;
            case 2:
                System.out.println("OPT 2 selected");
                MMServer.getInstance().sendStartDB();
                break;
            case 3:
                System.out.println("OPT 3 selected");
                break;
            case 4:
                MMServer.getInstance().sendStopDB();
                HadoopUtils.getInstance().closeResources();
                System.exit(0);
                break;
            default:
        }

        showMenu();
    }
}
