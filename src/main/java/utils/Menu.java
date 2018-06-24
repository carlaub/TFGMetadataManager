package utils;

import adapter.MetisAdapter;
import application.MetadataManager;
import constants.ErrorConstants;
import constants.MsgConstants;
import controllers.MMController;
import neo4j.GraphDatabase;
import network.MMServer;
import scala.reflect.internal.pickling.UnPickler;

import java.util.Scanner;

/**
 * Created by Carla Urrea Blázquez on 22/04/2018.
 *
 * Menu.java
 */

public class Menu {

    private static final int NUM_OPT = 4;

    private MMController mmController;
    private Scanner scan;

    public Menu() {
        this.mmController = new MMController();
    }

    /**
     * Show menu
     */
    public void showMenu() {
        scan = new Scanner(System.in);

        System.out.println(MsgConstants.MSG_MENU_TITLE);
        System.out.println(MsgConstants.MSG_MENU_OPT1);
        System.out.println(MsgConstants.MSG_MENU_OPT2);
        System.out.println(MsgConstants.MSG_MENU_OPT3);
        System.out.println(MsgConstants.MSG_MENU_OPT4);

        System.out.println(MsgConstants.MSG_SEL_OPT);

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
                System.out.println(MsgConstants.MSG_PROC_QUERIES);
                mmController.queriesFileExecution();
                System.out.println(MsgConstants.MSG_END_PROC_QUERIES);
                break;
            case 4:
                mmController.shutdownSystem();
                break;
            default:
        }

        showMenu();
    }

    private String readInput() {
        // TODO: Añadir comprobaciones
        return scan.nextLine();
    }
}
