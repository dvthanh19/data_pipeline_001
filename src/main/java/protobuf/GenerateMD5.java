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




public class GenerateMD5 {
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
    HashMap<String, String> dict = new HashMap<>();
    
    String directoryPath = "D:\\";
    File directory = new File(directoryPath);
    
    if (directory.exists() && directory.isDirectory()) {
      String[] files = directory.list();
      if (files != null) {
        for (String file : files) {
          File filePath = new File(directoryPath + file);
          if (!filePath.isDirectory())
          {
            dict.put(file, getMD5Hash(directoryPath + file));
            System.out.println(file + ": " + dict.get(file));
          }
        }
      } else {
        System.out.println("No files found in the directory.");
      }
    } else {
      System.err.println("Error: Directory not found or not a directory: " + directoryPath);
    }

    String hashFilePath = "./target/hashID.ser";
    writeToSerializedFile(dict, hashFilePath);
  }
}