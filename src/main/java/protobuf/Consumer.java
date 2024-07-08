package protobuf;

import protobuf.EmployeesOuterClass.Employees;
import protobuf.FileSchema.*;
import protobuf.GenerateMD5;

import java.util.LinkedList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.Stack;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;


public class Consumer {
    
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "video-topic";
    private static long expectedChunk = 0;
    private static HashMap<String, LinkedList<Datachunk>> fileMap = new HashMap<>();
    private static HashMap<String, FileOutputStream> outputStreamMap = new HashMap<>();
    private static LinkedList<Datachunk> chunkList = new LinkedList<>();


    public static void main(String[] args) throws IOException, NoSuchAlgorithmException
    {
        // -------------------------------------------------------------------------------------------
        // GET INFORMATION ABOUT SENT FILES ----------------------------------------------------------
        String trackFilePath = "D:\\server\\trackingFile.txt";
        HashMap<String, String> trackFile = null;
        Stack<String> stack = new Stack<>();
        
        try {trackFile = readFromSerializedFile(trackFilePath);} 
        catch (IOException e) 
        {System.err.println("Error reading file: " + e.getMessage());} 
        catch (ClassNotFoundException e)
        {System.err.println("Error: Class not found during deserialization: " + e.getMessage());}


        for (String file : trackFile.keySet())
            if (trackFile.get(file).charAt(0) != '1')
                stack.push(file);
        
        int stackSize = stack.size();
        for (int i = 0; i < stackSize; i += 1)
            trackFile.remove(stack.pop());

        // for (String file : trackFile.keySet())
        //     System.out.println("File <" + file + ">         " + trackFile.get(file));



        // -------------------------------------------------------------------------------------------
        // CREATE OUTPUTSTREAM FOR EACH FILE ---------------------------------------------------------

        // String filePath = "D:\\dir_server\\" + fileName + fileExt;
        String dir = "D:\\dir_server\\"; 

        
        for (String file : trackFile.keySet())
        {
            File outputFile = new File(dir + file);
            try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
                outputStreamMap.put(file, outputStream);
            } catch (IOException e) {e.printStackTrace();}
        }


        System.out.println("[Consumer] Starting...");


        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<Long, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));


        

        while (true) {
            boolean fun = true;
            HashMap<String, Integer> chunknumMap = new HashMap<>();
            HashMap<String, Integer> countMap = new HashMap<>();
            HashMap<String, Boolean> isFirstMap = new HashMap<>();
            for (String file : trackFile.keySet())
            {
                chunknumMap.put(file, 0);
                countMap.put(file, 0);
                isFirstMap.put(file, false);
            }


            ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(50));

            if (fun) 
            {
                fun = false;
                System.out.println("----------------- ... -----------------");
            }
        
            for (ConsumerRecord<Long, byte[]> record : records) 
            {
                byte[] message = record.value();
                Datachunk chunk = Datachunk.parseFrom(message);
                String fileName = chunk.getFileName() + chunk.getFileExt();

                if (isFirstMap.get(fileName)) {
                    chunknumMap.put(fileName, chunk.getChunkNum());
                    isFirstMap.put(fileName, false);
                }
                

                System.out.printf("Received message: offset = %d, partition = %d, message = %s\n",
                        record.offset(), record.partition(), chunk);

        //         if (expectedChunk == Long.parseLong(chunk.getChunkID()))
        //         {
        //             count += 1;
        //             outputStream.write(chunk.getBody().toByteArray());
        //             expectedChunk += 1;
                    
        //             if (chunkList.size() > 0)
        //             {
        //                 Datachunk tmp = chunkList.getFirst();
        //                 if (expectedChunk == Long.parseLong(tmp.getChunkID()))
        //                 {
        //                     while (expectedChunk == Long.parseLong(tmp.getChunkID()))
        //                     {
        //                         outputStream.write(tmp.getBody().toByteArray());
        //                         chunkList.removeFirst();

        //                         expectedChunk += 1;
        //                         if (chunkList.size() > 0)
        //                         {
        //                             tmp = chunkList.getFirst();
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //         else
        //         {
        //             // count += 1; //  ???
        //             boolean successFlag = addChunkIntoList(chunk);
        //             if (successFlag == true){
        //                 count += 1; //  ???
        //             }
        //         }
        //         System.out.println("count: " + count + ",      expected: " + expectedChunk  + ",      size: " + chunkList.size());
                
        //     }
        //     if (chunknum == count && !isFirst) {
        //         System.out.println("Done receiving: " + String.valueOf(chunknum) + "/" + String.valueOf(chunknum));

        //         int listSize = chunkList.size();
        //         for (int i = 0; i < listSize; i += 1)
        //         {
        //             expectedChunk += 1;
        //             Datachunk tmp = chunkList.getFirst();
        //             outputStream.write(tmp.getBody().toByteArray());

        //             chunkList.removeFirst();
        //         }

        //         System.out.println("-- expected: " + expectedChunk);
        //         break;
            }
        }
        consumer.close();

        
    }


    private static boolean addChunkIntoList(Datachunk chunk)
    {
        int size = chunkList.size();
        if (size == 0)
        {
            chunkList.add(0, chunk);  
            return true;
        }

        long id = Long.parseLong(chunk.getChunkID());


        // int i = size/2;
        // int k = 0;
        // Datachunk tmp = chunkList.get(size/2);

        // if (id < Long.parseLong(tmp.getChunkID())) 
        //     k = -1;
        // else k = 1;

        // for (;i >= 0 && i < chunkList.size(); i += k)
        // {
        //     tmp = chunkList.get(i);
        //     if (k == -1 && Long.parseLong(tmp.getChunkID()) < id)
        //     {
        //         i += 1;
        //         break;
        //     }
        //     else if (k == 1 && Long.parseLong(tmp.getChunkID()) > id)
        //     {
        //         break;
        //     }
        // }

        // if (i < 0) // The chunkID is smaller than headID
        // { 
        //     return false;
        // }

        // // alternative --------------
        int i = 0;
        Datachunk tmp;
        for (; i < chunkList.size(); i++)
        {
            tmp = chunkList.get(i);
            if (id < Long.parseLong(tmp.getChunkID()))
                break;
        }


        // System.out.println(i);
        chunkList.add(i, chunk);
        return true;
    }

    public static HashMap<String, String> readFromSerializedFile(String filePath) throws IOException, ClassNotFoundException {
        try (FileInputStream fileInputStream = new FileInputStream(filePath);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
        return (HashMap<String, String>) objectInputStream.readObject();
        }
    }

}
