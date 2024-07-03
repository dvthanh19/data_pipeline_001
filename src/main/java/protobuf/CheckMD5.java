package protobuf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;




public class CheckMD5 {
  public static String getMD5Hash(String filePath) throws IOException, NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    try (FileInputStream fis = new FileInputStream(filePath)) {
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = fis.read(buffer)) != -1) {
        md.update(buffer, 0, bytesRead);
      }
    }

    byte[] digest = md.digest();
    StringBuffer sb = new StringBuffer();
    for (byte b : digest) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  public static boolean checkFileIntegrity(String filePath, String referenceHash) throws IOException, NoSuchAlgorithmException {
    String calculatedHash = getMD5Hash(filePath);
    
    return calculatedHash.equals(referenceHash);
  }

  public static boolean isFileOrDirectory(String path) {
    File file = new File(path);
    return file.exists() && (file.isFile() || file.isDirectory());
  }
  
  public static boolean isFile(String path) {
    File file = new File(path);
    return file.exists() && file.isFile();
  }

  public static void writeToSerializedFile(HashMap<String, String> map, String filePath) throws IOException {
    try (
      FileOutputStream fileOutputStream = new FileOutputStream(filePath);
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) 
    {
      objectOutputStream.writeObject(map);
    }
  }

  public static HashMap<String, String> readFromSerializedFile(String filePath) throws IOException, ClassNotFoundException {
    try (FileInputStream fileInputStream = new FileInputStream(filePath);
         ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
      return (HashMap<String, String>) objectInputStream.readObject();
    }
  }

  public static void main(String[] args) throws IOException, NoSuchAlgorithmException {

    String hashFilePath = "./target/hashID.ser";
    HashMap<String, String> loadedMap = null;
    
    try {
      loadedMap = readFromSerializedFile(hashFilePath);
      if (loadedMap != null) {
        // System.out.println("Loaded HashMap:");
        // for (String key : loadedMap.keySet()) {
        //   System.out.println("\t" + key + ": " + loadedMap.get(key));
        // }
      } else {
        System.err.println("Error: HashMap might be null.");
      }
    } catch (IOException e) {
      System.err.println("Error reading file: " + e.getMessage());
    } catch (ClassNotFoundException e) {
      System.err.println("Error: Class not found during deserialization: " + e.getMessage());
    }



    // String checkPath = "D:\\";


    // String checkFile = "private_note1.txt";
    // String referenceHash = loadedMap.get(checkFile);
    
    // if (checkFileIntegrity(checkPath + checkFile, referenceHash)) {
    //   System.out.println("File <" + checkFile + "> integrity verified. Hash matches reference.");
    // } else {
    //   System.err.println("File <" + checkFile + "> integrity compromised. Hashes don't match!");
    // }


    
    String checkPath = "D:\\";
    File dir = new File(checkPath);

    if (dir.exists() && dir.isDirectory()) {
      String[] files = dir.list();
      if (files != null) {
        for (String file : files) {
          File filePath = new File(checkPath + file);
          if (!filePath.isDirectory())
          {
            
            String refHash = loadedMap.get(file);
            // System.out.println(file + ": " + refHash);
            if (refHash == null){
              System.out.println("File <" + file + "> was added.");
            }
            else{
              if (!checkFileIntegrity(checkPath + file, refHash)) {
                System.out.println("File <" + file + "> was modified.");
              }
              else {
                System.out.println("File <" + file + "> is up to date.");
              } 
            }
          }
        }
      } else {
        System.out.println("No files found in the directory.");
      }
    } else {
      System.err.println("Error: Directory not found or not a directory: " + checkPath);
    }
  }
}