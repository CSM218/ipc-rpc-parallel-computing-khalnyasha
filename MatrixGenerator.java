package pdc;
import java.util.Random;

public class MatrixGenerator {
    public static int[][] generateRandomMatrix(int r, int c, int max) {
        int[][] m = new int[r][c];
        Random rnd = new Random();
        for(int i=0; i<r; i++) for(int j=0; j<c; j++) m[i][j] = rnd.nextInt(max);
        return m;
    }

    public static String serialize(int[][] m) {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<m.length; i++) {
            for(int j=0; j<m[i].length; j++) sb.append(m[i][j]).append(j < m[i].length-1 ? "," : "");
            if(i < m.length-1) sb.append("\\");
        }
        return sb.toString();
    }

    public static int[][] parse(String s) {
        String[] rows = s.split("\\\\");
        int[][] m = new int[rows.length][rows[0].split(",").length];
        for(int i=0; i<rows.length; i++) {
            String[] cols = rows[i].split(",");
            for(int j=0; j<cols.length; j++) m[i][j] = Integer.parseInt(cols[j]);
        }
        return m;
    }
}