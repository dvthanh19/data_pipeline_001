package protobuf;

import protobuf.EmployeesOuterClass.Employees;
import protobuf.FileSchema.*;
import protobuf.GenerateMD5;

import java.util.LinkedList;
import java.util.Queue;

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
    private static HashMap<String, LinkedList<Datachunk>> chunkListMap = new HashMap<>();
    private static HashMap<String, FileOutputStream> outputStreamMap = new HashMap<>();
    // private static LinkedList<Datachunk> chunkList = new LinkedList<>();


    public static void main(String[] args) throws IOException, NoSuchAlgorithmException
    {
        // -------------------------------------------------------------------------------------------
        // GET INFORMATION ABOUT SENT FILES ----------------------------------------------------------
        String trackFilePath = "D:\\server\\trackingFile.txt";
        HashMap<String, String> trackFiles = null;
        Stack<String> stack = new Stack<>();
        int stackSize = 0;
        
        try {trackFiles = readFromSerializedFile(trackFilePath);} 
        catch (IOException e) 
        {System.err.println("Error reading file: " + e.getMessage());} 
        catch (ClassNotFoundException e)
        {System.err.println("Error: Class not found during deserialization: " + e.getMessage());}


        for (String file : trackFiles.keySet())
            System.out.println("File <" + file + ">         " + trackFiles.get(file));
            System.out.println("      -----------      ");
        // Push all file with status xy (x!=1) into stack
        for (String file : trackFiles.keySet())
            if (trackFiles.get(file).charAt(0) != '1')
                stack.push(file);
        
        // Remove all file with status xy (x!=1) out of hashMap
        stackSize = stack.size();
        for (int i = 0; i < stackSize; i += 1)
            trackFiles.remove(stack.pop());

        for (String file : trackFiles.keySet())
            System.out.println("File <" + file + ">         " + trackFiles.get(file));



        // -------------------------------------------------------------------------------------------
        // CREATE OUTPUTSTREAM FOR EACH FILE ---------------------------------------------------------

        // String filePath = "D:\\dir_server\\" + fileName + fileExt;
        String dir = "D:\\dir_server\\"; 
        HashMap<String, Long> chunknumMap = new HashMap<>();
        HashMap<String, Long> countMap = new HashMap<>();
        HashMap<String, Long> expectedChunkMap = new HashMap<>();
        HashMap<String, Boolean> isFirstMap = new HashMap<>();
        HashMap<String, Boolean> receiveMap = new HashMap<>();
        boolean fun = true;
        FileOutputStream outputStream1 = new FileOutputStream(new File(dir + "test.txt"));;
        
        for (String file : trackFiles.keySet())
        {
            File outputFile = new File(dir + file);
            System.out.println(dir + file);
            try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
                // outputStreamMap.put(file, outputStream);
                // outputStream1 = new FileOutputStream(outputFile);

            } catch (IOException e) {e.printStackTrace();}

            chunknumMap.put(file, (long) 0);
            countMap.put(file, (long) 0);
            expectedChunkMap.put(file, (long) 0);

            isFirstMap.put(file, true);
            receiveMap.put(file, false);

            chunkListMap.put(file, new LinkedList<>());
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


        String fileName;
        while (true) {
            // System.out.println("...1");
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
                fileName = chunk.getFileName() + chunk.getFileExt();

                if (isFirstMap.get(fileName))
                {
                    chunknumMap.put(fileName, chunk.getChunkNum());
                    isFirstMap.put(fileName, false);
                }
                

                System.out.printf("Received message: offset = %d, partition = %d, message = %s\n",
                        record.offset(), record.partition(), chunk);

                if (expectedChunkMap.get(fileName) == Long.parseLong(chunk.getChunkID()))
                {
                    System.out.println("... 1");
                    countMap.put(fileName, countMap.get(fileName) + 1);
                    // outputStreamMap.get(fileName).write(chunk.getBody().toByteArray());
                    outputStream1.write(chunk.getBody().toByteArray());
                    System.out.println("... 2");
                    expectedChunkMap.put(fileName, expectedChunkMap.get(fileName) + 1);
                    

                    if (chunkListMap.get(fileName).size() > 0)
                    {
                        Datachunk tmp = chunkListMap.get(fileName).getFirst();
                        if (expectedChunkMap.get(fileName) == Long.parseLong(tmp.getChunkID()))
                        {
                            while (expectedChunkMap.get(fileName) == Long.parseLong(tmp.getChunkID()))
                            {
                                outputStreamMap.get(fileName).write(tmp.getBody().toByteArray());
                                chunkListMap.get(fileName).removeFirst();

                                expectedChunkMap.put(fileName, expectedChunkMap.get(fileName) + 1);
                                if (chunkListMap.get(fileName).size() > 0)
                                {
                                    tmp = chunkListMap.get(fileName).getFirst();
                                }
                            }
                        }
                    }
                }
                else
                {
                    // count += 1; //  ???
                    boolean successFlag = addChunkIntoList(chunk, chunkListMap.get(fileName));
                    if (successFlag == true){
                        countMap.put(fileName, countMap.get(fileName) + 1); //  ???
                    }
                }
                System.out.println("count: " + countMap.get(fileName) + ",      expected: " + expectedChunkMap.get(fileName)  + ",      llsize: " + chunkListMap.get(fileName).size());
                
            }

            int count = 0;
            // System.out.println("...2");
            for (String file : trackFiles.keySet())
            {   
                if (!receiveMap.get(file))
                {
                    System.out.println(chunknumMap.get(file) + "    " + countMap.get(file) + "    " + !isFirstMap.get(file));// ----
                    if (chunknumMap.get(file) == countMap.get(file) && !isFirstMap.get(file)) {
                        System.out.println("...2.2   " + chunknumMap.get(file));
                        count += 1;
                        

                        System.out.println("Done receiving: " + String.valueOf(countMap.get(file)) + "/" + String.valueOf(chunknumMap.get(file)));
        
                        int listSize = chunkListMap.get(file).size();
                        for (int i = 0; i < listSize; i += 1)
                        {
                            expectedChunkMap.put(file, expectedChunkMap.get(file) + 1) ;
                            Datachunk tmp = chunkListMap.get(file).getFirst();
                            outputStreamMap.get(file).write(tmp.getBody().toByteArray());
        
                            chunkListMap.get(file).removeFirst();
                        }
        
                        System.out.println("-- expected: " + expectedChunkMap.get(file));
                        receiveMap.put(file, true);
                    }
                }
                else {count += 1;}
            }
            // System.out.println("...3");

            System.out.println(count + "   " + trackFiles.size());
            if (count == trackFiles.size()) break;
        }


        consumer.close();
    }


    private static boolean addChunkIntoList(Datachunk chunk, LinkedList<Datachunk> chunkList)
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
