package utils;

/**
 * Created by Carla Urrea Blázquez on 22/04/2018.
 *
 * Checker.java
 */
public class Checker {

    public static boolean checkPositiveNumber(String sample) {
        String pattern = "^[1-9]\\d*$";
        return sample.matches(pattern);
    }

}
