package protobuf;

import protobuf.EmployeesOuterClass.Employees;
import protobuf.FileSchema.*;
import protobuf.GenerateMD5;

import java.util.LinkedList;

import java.io.IOException;
import java.io.File;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Properties;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.google.protobuf.ByteString;


public class Kafka_consumer {
    
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "video-topic";
    // private static final String FILE_NAME = "consumed_messages.txt";
    private static long expectedChunk = 0;
    private static LinkedList<Datachunk> chunkList = new LinkedList<>();

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
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


        String fileName = "test";
        String fileExt = ".txt";
        String filePath = "D:\\dir_server\\" + fileName + fileExt;
        File outputFile = new File(filePath);
        long chunknum = 0;
        long count = 0;
        boolean isFirst = true;

        


        try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
            while (true) {
                ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(50));
                for (ConsumerRecord<Long, byte[]> record : records) {
                    byte[] message = record.value();
                    Datachunk chunk = Datachunk.parseFrom(message);
                    if (isFirst) {
                        // System.out.println("123");
                        chunknum = chunk.getChunkNum();
                        // System.out.println(chunknum);
                        isFirst = false;
                    }
                    

                    System.out.printf("Received message: offset = %d, partition = %d, message = %s\n",
                            record.offset(), record.partition(), chunk);

                    if (expectedChunk == Long.parseLong(chunk.getChunkID()))
                    {
                        count += 1;
                        outputStream.write(chunk.getBody().toByteArray());
                        expectedChunk += 1;
                        
                        Datachunk tmp = chunkList.getFirst();
                        if (expectedChunk == Long.parseLong(tmp.getChunkID()))
                        {
                            while (expectedChunk == Long.parseLong(tmp.getChunkID()))
                            {
                                outputStream.write(tmp.getBody().toByteArray());
                                chunkList.removeFirst();

                                tmp = chunkList.getFirst();
                                expectedChunk += 1;
                            }
                        }
                    }
                    else
                    {
                        boolean successFlag = addChunkIntoList(chunk);
                        if (successFlag == true){
                            count += 1;
                        }
                    }
                    
                }
                if (chunknum == count && !isFirst) {
                    System.out.println("Done receiving" + String.valueOf(chunknum) + "/" + String.valueOf(chunknum));

                    int listSize = chunkList.size();
                    for (int i = 0; i < listSize; i += 1)
                    {
                        Datachunk tmp = chunkList.getFirst();
                        outputStream.write(tmp.getBody().toByteArray());

                        chunkList.removeFirst();
                    }

                    break;
                }
            }
            consumer.close();
        } catch (IOException e) {
            
            e.printStackTrace();
        }

        
    }


    private static boolean addChunkIntoList(Datachunk chunk)
    {
        int size = chunkList.size();

        int i = size/2;


        while(i >= 0)
        {
            Datachunk tmp = chunkList.get(i);
            if (expectedChunk < Long.parseLong(tmp.getChunkID()))
            {
                i -= 1;
            }
            else if (expectedChunk > Long.parseLong(tmp.getChunkID()))
            {
                i += 1;
                break;
            }
            else
            {
                return false;
            }
        }

        if (i < 0) // The chunkID is smaller than headID
        { 
            return false;
        }

        chunkList.add(i, chunk);        
        return true;
    }
}
