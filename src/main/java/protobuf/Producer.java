package protobuf;

import protobuf.FileSchema.*;
import protobuf.GenerateMD5.*;
import protobuf.CheckMD5.*;
import java.util.Stack;

import java.io.IOException;
import java.io.File;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Properties;
import java.security.NoSuchAlgorithmException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.protobuf.ByteString;


public class Producer {
    
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Update with your Kafka broker address
    private static final String TOPIC_NAME  = "video-topic";
    private static final int partition_num  = 5;



    public static void main(String[] args) throws IOException, NoSuchAlgorithmException 
    {
        String trackFilePath = "D:\\server\\trackingFile.txt";
        HashMap<String, String> trackFile = null;
        Stack<String> stack = new Stack<>();
        
        try {trackFile = readFromSerializedFile(trackFilePath);} 
        catch (IOException e) 
        {System.err.println("Error reading file: " + e.getMessage());} 
        catch (ClassNotFoundException e)
        {System.err.println("Error: Class not found during deserialization: " + e.getMessage());}


        for (String file : trackFile.keySet())
        {
            if (trackFile.get(file).charAt(0) != '1')
                stack.push(file);
        }
        
        int stackSize = stack.size();
        for (int i = 0; i < stackSize; i += 1)
            trackFile.remove(stack.pop());

        for (String file : trackFile.keySet())
            System.out.println("File <" + file + ">         " + trackFile.get(file));
        
        
        System.out.println("-----------------");







        System.out.println("[Producer] Starting...");
        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // Replace with your broker address
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());


        // System.out.println("[Producer] Starting...");
        // // int chunkSize = 1024;//1024 * 1024 - 200; // 1 MB chunks
        int chunkSize = 1024 * 1024 - 200; // 1 MB chunks

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props))
        {
            for (String file : trackFile.keySet())
            {
                // File details
                String fileName = file.substring(0, file.lastIndexOf('.'));
                String fileExt = file.substring(file.lastIndexOf('.'));
                String filePath = "D:\\dir_client\\" + fileName + fileExt;

                File nfile = new File(filePath);
                float fileSize = nfile.length();


                try(
                    FileInputStream inputStream = new FileInputStream(filePath);
                    BufferedInputStream bis = new BufferedInputStream(inputStream))
                {
                
                    byte[] chunk = new byte[chunkSize];
                    int bytesRead;
                    int chunkNum = (int) Math.ceil(fileSize / chunkSize);
                    // System.out.println(chunkNum);

                    int chunkCount = 0;

                    
                    System.out.println("Start sending -  " + fileName + fileExt + " ...");
                    while ((bytesRead = bis.read(chunk)) > 0) {
                        // Create a new byte array to avoid exceeding chunk size
                        byte[] tmp = new byte[bytesRead];
                        System.arraycopy(chunk, 0, tmp, 0, bytesRead);

                        Datachunk datachunk = Datachunk.newBuilder()
                            .setFileHash(GenerateMD5.getMD5Hash(filePath))
                            .setFileName(fileName)
                            .setFileExt(fileExt)
                            .setFileSize(fileSize)
                            .setChunkSize(chunkSize)
                            .setChunkNum(chunkNum)
                            .setChunkID(String.valueOf(chunkCount))
                            .setBody(ByteString.copyFrom(tmp))
                            .build();

                        byte[] chunkToSend = datachunk.toByteArray();

                        // Create a Kafka message with a key (optional) and the chunk data
                        // System.out.println(1);
                        String key = String.valueOf(Long.parseLong(datachunk.getChunkID()) % partition_num);
                        // String key = datachunk.getFileName();
                        // System.out.println(key);

                        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC_NAME, key, chunkToSend);
                        producer.send(record);
                
                        chunkCount++;
                    }
                }
            }
        }




        System.out.println("Done sending files");
    }


    
    public static void kafkaProduce() throws IOException, NoSuchAlgorithmException {
        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // Replace with your broker address
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        


        // File details
        String fileName = "vid";
        String fileExt = ".mp4";
        String filePath = "D:\\dir_client\\" + fileName + fileExt;
        File file = new File(filePath);
        float fileSize = file.length();
        // System.out.println(fileSize);


        System.out.println("[Producer] Starting...");
        // int chunkSize = 1024;//1024 * 1024 - 200; // 1 MB chunks
        int chunkSize = 1024 * 1024 - 200; // 1 MB chunks

        try (
            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
            FileInputStream inputStream = new FileInputStream(filePath);
            BufferedInputStream bis = new BufferedInputStream(inputStream)) 
        {
            byte[] chunk = new byte[chunkSize];
            int bytesRead;
            int chunkNum = (int) Math.ceil(fileSize / chunkSize);
            // System.out.println(chunkNum);

            int chunkCount = 0;

            
            System.out.println("Start sending file...");
            while ((bytesRead = bis.read(chunk)) > 0) {
                // Create a new byte array to avoid exceeding chunk size
                byte[] tmp = new byte[bytesRead];
                System.arraycopy(chunk, 0, tmp, 0, bytesRead);

                Datachunk datachunk = Datachunk.newBuilder()
                    .setFileHash(GenerateMD5.getMD5Hash(filePath))
                    .setFileName(fileName)
                    .setFileExt(fileExt)
                    .setFileSize(fileSize)
                    .setChunkSize(chunkSize)
                    .setChunkNum(chunkNum)
                    .setChunkID(String.valueOf(chunkCount))
                    .setBody(ByteString.copyFrom(tmp))
                    .build();

                byte[] chunkToSend = datachunk.toByteArray();

                // Create a Kafka message with a key (optional) and the chunk data
                // System.out.println(1);
                String key = String.valueOf(Long.parseLong(datachunk.getChunkID()) % partition_num);
                // String key = datachunk.getFileName();
                // System.out.println(key);

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC_NAME, key, chunkToSend);
                producer.send(record);
        
                chunkCount++;
            }
        }

        System.out.println("Done sending files");
    }




    
    public static HashMap<String, String> readFromSerializedFile(String filePath) throws IOException, ClassNotFoundException {
        try (FileInputStream fileInputStream = new FileInputStream(filePath);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
        return (HashMap<String, String>) objectInputStream.readObject();
        }
    }

}
