package protobuf;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.lang.InterruptedException;

public class Main_flow {
    public static void main(String[] args)
    {   
        System.out.println("------------------------------------------------------------------------------------");
        System.out.println("MD5 processing ---------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------------------------\n");

        // Generate MD5
        try{GenerateMD5.generateMD5();}
        catch (NoSuchAlgorithmException e1) {e1.printStackTrace();}
        catch (IOException e2) {e2.printStackTrace();}

        // Check MD5 between client and server
        try{CheckMD5.checkMD5();}
        catch (NoSuchAlgorithmException e1) {e1.printStackTrace();}
        catch (IOException e2) {e2.printStackTrace();}



        System.out.println("\n------------------------------------------------------------------------------------");
        System.out.println("File transmitting ------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------------------------\n");

        ComsumerThread consumerThread = new ComsumerThread();
        ProducerThread producerThread = new ProducerThread();

        consumerThread.start();
        try {
            Thread.sleep(15000);
        } catch (InterruptedException  e) {
            e.printStackTrace();
        }
        producerThread.start();

        try {
            consumerThread.join();
            producerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        System.out.println("\n------------------------------------------------------------------------------------");
        System.out.println("MD5 processing ---------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------------------------\n");

        // Generate MD5
        try{GenerateMD5.generateMD5();}
        catch (NoSuchAlgorithmException e1) {e1.printStackTrace();}
        catch (IOException e2) {e2.printStackTrace();}

        // Check MD5 between client and server
        try{CheckMD5.checkMD5();}
        catch (NoSuchAlgorithmException e1) {e1.printStackTrace();}
        catch (IOException e2) {e2.printStackTrace();}

    }


    public static void  dir_processing()
    {
        System.out.println("------------------------------------------------------------------------------------");
        System.out.println("MD5 processing ---------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------------------------\n");

        // Generate MD5
        try{GenerateMD5.generateMD5();}
        catch (NoSuchAlgorithmException e1) {e1.printStackTrace();}
        catch (IOException e2) {e2.printStackTrace();}

        // Check MD5 between client and server
        try{CheckMD5.checkMD5();}
        catch (NoSuchAlgorithmException e1) {e1.printStackTrace();}
        catch (IOException e2) {e2.printStackTrace();}



        System.out.println("\n------------------------------------------------------------------------------------");
        System.out.println("File transmitting ------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------------------------\n");


        System.out.println("[Mainflow] ComsumerThread start");
        ComsumerThread consumerThread = new ComsumerThread();

        System.out.println("[Mainflow] ComsumerThread start");
        ProducerThread producerThread = new ProducerThread();


        consumerThread.start();
        try {
            Thread.sleep(15000);
        } catch (InterruptedException  e) {
            e.printStackTrace();
        }
        producerThread.start();

        try {
            consumerThread.join();
            producerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        System.out.println("\n------------------------------------------------------------------------------------");
        System.out.println("MD5 processing ---------------------------------------------------------------------");
        System.out.println("------------------------------------------------------------------------------------\n");

        // Generate MD5
        try{GenerateMD5.generateMD5();}
        catch (NoSuchAlgorithmException e1) {e1.printStackTrace();}
        catch (IOException e2) {e2.printStackTrace();}

        // Check MD5 between client and server
        try{CheckMD5.checkMD5();}
        catch (NoSuchAlgorithmException e1) {e1.printStackTrace();}
        catch (IOException e2) {e2.printStackTrace();}

    }
}


class ComsumerThread extends Thread {
    public void run() {
        try{Consumer.consume();}
        catch (NoSuchAlgorithmException e1) {e1.printStackTrace(); System.out.println("Consumer: e1");}
        catch (IOException e2) {e2.printStackTrace();System.out.println("Consumer: e2");}
    }
}

class ProducerThread extends Thread {
    public void run() {
        try{Producer.produce();}
        catch (NoSuchAlgorithmException e1) {e1.printStackTrace(); System.out.println("Consumer: e1");}
        catch (IOException e2) {e2.printStackTrace();System.out.println("Consumer: e2");}
    }
}