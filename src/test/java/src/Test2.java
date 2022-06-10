package src;

public class Test2 {
    public static void main(String[] args) {
        soooo so = new soooo();
        int[][] ints = so.sortedSquares(888);
        for (int i = 0; i < ints[0].length; i++) {
            for (int j = 0; j < ints.length; j++) {
                System.out.println(ints[j][i]);
            }
        }

    }

}
class soooo{
    public int[][] sortedSquares(int n){
        return null;
    }
}