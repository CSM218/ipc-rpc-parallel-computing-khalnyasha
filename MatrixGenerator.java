package pdc;

import java.util.Random;

/**
 * Utility class for generating and manipulating matrices.
 * Provides helper methods for creating test and example matrices.
 */
public class MatrixGenerator {

    private static final Random random = new Random();

    public static int[][] generateRandomMatrix(int rows, int cols, int maxValue) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = random.nextInt(maxValue);
            }
        }
        return matrix;
    }

    public static int[][] generateIdentityMatrix(int size) {
        int[][] matrix = new int[size][size];
        for (int i = 0; i < size; i++) {
            matrix[i][i] = 1;
        }
        return matrix;
    }

    public static int[][] generateFilledMatrix(int rows, int cols, int value) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = value;
            }
        }
        return matrix;
    }

    /**
     * Serializes a matrix into the custom string format: 1,2\3,4
     */
    public static String serialize(int[][] matrix) {
        if (matrix == null) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                sb.append(matrix[i][j]);
                if (j < matrix[i].length - 1) sb.append(",");
            }
            if (i < matrix.length - 1) sb.append("\\");
        }
        return sb.toString();
    }

    /**
     * Parses the string format back into an int[][] matrix
     */
    public static int[][] parse(String data) {
        if (data == null || data.isEmpty()) return new int[0][0];
        String[] rows = data.split("\\\\");
        int rowCount = rows.length;
        int colCount = rows[0].split(",").length;
        int[][] matrix = new int[rowCount][colCount];
        for (int i = 0; i < rowCount; i++) {
            String[] cols = rows[i].split(",");
            for (int j = 0; j < cols.length; j++) {
                matrix[i][j] = Integer.parseInt(cols[j]);
            }
        }
        return matrix;
    }

    public static void printMatrix(int[][] matrix, String label) {
        if (label != null && !label.isEmpty()) System.out.println(label);
        for (int[] row : matrix) {
            for (int val : row) {
                System.out.printf("%6d ", val);
            }
            System.out.println();
        }
    }

    public static void printMatrix(int[][] matrix) {
        printMatrix(matrix, "");
    }
}