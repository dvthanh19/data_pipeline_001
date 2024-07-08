package protobuf;

import protobuf.EmployeesOuterClass.Employee;
import protobuf.FileSchema.*;

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
  public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
    String dirName = "dir_client";
    String dir = "D:\\" + dirName + "\\";
    String hashFilePath = "D:\\server\\MD5_file.ser";

    File directory = new File(dir);
    HashMap<String, String> dict = new HashMap<>();
    
    
    if (directory.exists() && directory.isDirectory()) {
      String[] files = directory.list();
      if (files != null) {
        for (String file : files) {
          File filePath = new File(dir + file);

          if (!filePath.isDirectory())
          {
            dict.put(file, getMD5Hash(dir + file));
            System.out.println(file + ": " + dict.get(file));
          }
        }
      } else {
        System.out.println("No files found in the directory.");
      }
    } else {
      System.err.println("Error: Directory not found or not a directory: " + dir);
    }

    
    writeToSerializedFile(dict, hashFilePath);
  }

  public static void generateMD5() throws IOException, NoSuchAlgorithmException 
  {    
    String dirName = "dir_client";
    String dir = "D:\\" + dirName + "\\";
    String hashFilePath = "D:\\server\\MD5_file.ser";

    File directory = new File(dir);
    HashMap<String, String> dict = new HashMap<>();
    
    
    if (directory.exists() && directory.isDirectory()) {
      String[] files = directory.list();
      if (files != null) {
        for (String file : files) {
          File filePath = new File(dir + file);

          if (!filePath.isDirectory())
          {
            dict.put(file, getMD5Hash(dir + file));
            System.out.println(file + ": " + dict.get(file));
          }
        }
      } else {
        System.out.println("No files found in the directory.");
      }
    } else {
      System.err.println("Error: Directory not found or not a directory: " + dir);
    }

    
    writeToSerializedFile(dict, hashFilePath);
  }




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

  public static void writeToSerializedFile(HashMap<String, String> map, String filePath) throws IOException {
    try (
      FileOutputStream fileOutputStream = new FileOutputStream(filePath);
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) 
    {
      objectOutputStream.writeObject(map);
    }
  }

  public static void writeToFile(DirType dirObj, String filePath) throws IOException {
    byte[] binData = dirObj.toByteArray();
    File outputMD5 = new File(filePath);

    try (FileOutputStream outputStream = new FileOutputStream(outputMD5);) 
    {
      outputStream.write(binData);
    } catch (IOException e) 
    {
      e.printStackTrace();
    }
  }

  public static HashMap<String, String> readFromSerializedFile(String filePath) throws IOException, ClassNotFoundException {
    try (FileInputStream fileInputStream = new FileInputStream(filePath);
         ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
      return (HashMap<String, String>) objectInputStream.readObject();
    }
  }






  // public static void generateMD5() throws IOException, NoSuchAlgorithmException {
  //   String dirName = "dir_client";
  //   String dir = "D:\\" + dirName + "\\";
  //   String hashFilePath = "D:\\server\\MD5_file.ser";

  //   File directory = new File(dir);

  //   DirType myDir = DirType.newBuilder().setDirName(dirName).build();

    
  //   if (directory.exists() && directory.isDirectory()) 
  //   {
  //     String[] files = directory.list();
  //     if (files != null) 
  //     {
  //       for (String file : files) {
  //         File filePath = new File(dir + file);

  //         if (!filePath.isDirectory())
  //         {
  //           String md5 = getMD5Hash(dir + file);
  //           String fname = file.substring(0, file.lastIndexOf('.'));
  //           String fext = file.substring(file.lastIndexOf('.') + 1);

  //           // Add file into directory
  //           myDir = DirType.newBuilder()
  //           .addFiles(FileType.newBuilder()
  //           .setFileHash(md5)
  //           .setFileName(fname)
  //           .setFileExt(fext)
  //           .setTracked(false)
  //           .build()).build();
  //           System.out.println(file + ": " + md5);
  //         }
  //       }

        
  //     } else {
  //       System.out.println("No files found in the directory.");
  //     }
  //   } else {
  //     System.err.println("Error: Directory not found or not a directory: " + dir);
  //   }

  //   writeToFile(myDir, hashFilePath);
  // }

}