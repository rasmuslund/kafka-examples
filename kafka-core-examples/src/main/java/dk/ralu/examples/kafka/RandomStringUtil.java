package dk.ralu.examples.kafka;

import java.util.Random;

public final class RandomStringUtil {

    private RandomStringUtil() {
    }

    private static final Random RANDOM = new Random();

    public static String randomString(int lenght, char min, char max) {
        StringBuilder sb = new StringBuilder();
        for (int n = 0; n < lenght; n++) {
            sb.append(randomChar(min, max));
        }
        return sb.toString();
    }

    private static char randomChar(char min, char max) {
        if (min > max) {
            throw new IllegalArgumentException("Parameter min must be <= max (min was " + min + ", max was " + max + ")");
        }
        return (char) (RANDOM.nextInt((max + 1) - min) + min);
    }
}
