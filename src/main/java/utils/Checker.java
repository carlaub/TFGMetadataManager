package utils;

/**
 * Created by Carla Urrea Blázquez on 22/04/2018.
 **
 * This class contains functions useful for the verification of formats and types.
 */
public class Checker {

    /**
     * Check if [sample] is a positive number.
     *
     * @param sample is the number to verity
     * @return true if [sample] is positive
     */
    public static boolean checkPositiveNumber(String sample) {
        String pattern = "^[1-9]\\d*$";
        return sample.matches(pattern);
    }
}
