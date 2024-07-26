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

import com.google.protobuf.InvalidProtocolBufferException;

import protobuf.EmployeesOuterClass.Employees;
import protobuf.FileSchema.DirType;




public class CheckMD5 {
  public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
    String dirName = "dir_server";
    String checkPath = "D:\\" + dirName + "\\";
    String hashFilePath = "D:\\server\\MD5_file.ser";
    String trackFilePath = "D:\\server\\trackingFile.txt";

    File dir = new File(checkPath);
    HashMap<String, String> loadedMap = null;
    HashMap<String, String> tracking = new HashMap<>();
    
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


    // xy                   e.g: 00: untracked, 01: tracked and unchange
    // x: 0: not change     1: added          2: modified        3: removed
    // y: 0: untracked      1: tracked
    for (String key : loadedMap.keySet()) {
      tracking.put(key, "00");
    }


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
              tracking.put(file, "31");
              System.out.println("File <" + file + "> was removed.");
            }
            else
            {
              if (!checkFileIntegrity(checkPath + file, refHash)) {
                tracking.put(file, "21");
                System.out.println("File <" + file + "> was modified.");
              }
              else {
                tracking.put(file, "01");
                System.out.println("File <" + file + "> is up to date.");
              } 
            }
          }
        }

        for (String file : tracking.keySet()) 
        {
          if (tracking.get(file).charAt(1) == '0')
          {
            tracking.put(file, "11");
            System.out.println("File <" + file + "> was added.");
          } 
        }


      } else {
        System.out.println("No files found in the directory.");
      }
    } else {
      System.err.println("Error: Directory not found or not a directory: " + checkPath);
    }


    // System.out.println("\n- Tracking ----------------------------");
    // for (String file : tracking.keySet()) 
    // {
    //   System.out.println("<" + file + ">   " + tracking.get(file));
    // } 
    writeToSerializedFile(tracking, trackFilePath);
  }

  public static void checkMD5() throws IOException, NoSuchAlgorithmException {
    String dirName = "dir_server";
    String checkPath = "D:\\" + dirName + "\\";
    String hashFilePath = "D:\\server\\MD5_file.ser";
    String trackFilePath = "D:\\server\\trackingFile.txt";

    File dir = new File(checkPath);
    HashMap<String, String> loadedMap = null;
    HashMap<String, String> tracking = new HashMap<>();
    
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


    // xy                   e.g: 00: untracked, 01: tracked and unchange
    // x: 0: not change     1: added          2: modified        3: removed
    // y: 0: untracked      1: tracked
    for (String key : loadedMap.keySet()) {
      tracking.put(key, "00");
    }


    System.out.println("File status ---------------------------");
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
              tracking.put(file, "31");
              System.out.println("File <" + file + "> was removed (" + tracking.get(file) + ") .");
            }
            else
            {
              if (!checkFileIntegrity(checkPath + file, refHash)) {
                tracking.put(file, "21");
                System.out.println("File <" + file + "> was modified (" + tracking.get(file) + ") .");
              }
              else {
                tracking.put(file, "01");
                System.out.println("File <" + file + "> is up to date (" + tracking.get(file) + ") .");
              } 
            }
          }
        }

        for (String file : tracking.keySet()) 
        {
          if (tracking.get(file).charAt(1) == '0')
          {
            tracking.put(file, "11");
            System.out.println("File <" + file + "> was added (" + tracking.get(file) + ") .");
          } 
        }


      } else {
        System.out.println("No files found in the directory.");
      }
    } else {
      System.err.println("Error: Directory not found or not a directory: " + checkPath);
    }


    // System.out.println("\n- Tracking ----------------------------");
    // for (String file : tracking.keySet()) 
    // {
    //   System.out.println("<" + file + ">   " + tracking.get(file));
    // } 
    writeToSerializedFile(tracking, trackFilePath);
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

  public static HashMap<String, String> readFromSerializedFile(String filePath) throws IOException, ClassNotFoundException {
    try (FileInputStream fileInputStream = new FileInputStream(filePath);
         ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
      return (HashMap<String, String>) objectInputStream.readObject();
    }
  }

  public static DirType readFromFile(String filePath) throws IOException, ClassNotFoundException 
  {
    File inputMD5 = new File(filePath);

    try (FileInputStream inputStream = new FileInputStream(inputMD5);) 
    {
      byte[] fileData = inputStream.readAllBytes();
      inputStream.close();
      DirType dirObj = DirType.parseFrom(fileData);
      return dirObj;
    }
  }

  public static void writeToSerializedFile(HashMap<String, String> map, String filePath) throws IOException {
    try (
      FileOutputStream fileOutputStream = new FileOutputStream(filePath);
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) 
    {
      objectOutputStream.writeObject(map);
    }
  }


  // public static void checkMD5() throws IOException, NoSuchAlgorithmException 
  // {
    
  //   String dirName = "dir_sever";
  //   String checkPath = "D:\\" + dirName + "\\";
  //   String hashFilePath = "D:\\server\\MD5_file.ser";

  //   File dir = new File(checkPath);

  //   DirType dirObj = null;





  //   try {
  //     dirObj = readFromFile(hashFilePath);
  //     if (dirObj.getFilesList().size() != 0) {
  //       // System.out.println("Loaded HashMap:");
  //       // for (String key : loadedMap.keySet()) {
  //       //   System.out.println("\t" + key + ": " + loadedMap.get(key));
  //       // }
  //     } else {
  //       System.err.println("Error: HashMap might be null.");
  //     }
  //   } catch (IOException e) {
  //     System.err.println("Error reading file: " + e.getMessage());
  //   } catch (ClassNotFoundException e) {
  //     System.err.println("Error: Class not found during deserialization: " + e.getMessage());
  //   }



  //   if (dir.exists() && dir.isDirectory()) {
  //     String[] files = dir.list();

  //     if (files != null) {
  //       for (String file : files) {
  //         File filePath = new File(checkPath + file);

  //         if (!filePath.isDirectory())
  //         {
  //           String refHash = getMD5FromObj(file, dirObj);


  //           if (refHash == null){
  //             System.out.println("File <" + file + "> was deleted.");
  //           }
  //           else
  //           {
  //             if (!checkFileIntegrity(checkPath + file, refHash)) {
  //               System.out.println("File <" + file + "> was modified.");
  //             }
  //             else {
  //               System.out.println("File <" + file + "> is up to date.");
  //             } 
  //           }
  //         }
  //       }



  //     } else {
  //       System.out.println("No files found in the directory.");
  //     }
  //   } else {
  //     System.err.println("Error: Directory not found or not a directory: " + checkPath);
  //   }
  // }

}