package protobuf;

public class test {
    public static void main(String[] args) {

        float a = 10;
        float b = 100;
        int chunkNum = (int) Math.ceil(a / b) - 1;
        System.out.println(chunkNum);
    }
}
