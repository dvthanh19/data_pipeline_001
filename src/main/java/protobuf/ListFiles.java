package protobuf;


import java.io.File;


public class ListFiles {

  public static void main(String[] args) {
    String directoryPath = "D:\\";
    File directory = new File(directoryPath);
    if (directory.exists() && directory.isDirectory()) {
      String[] files = directory.list();
      if (files != null) {
        for (String file : files) {
          System.out.println(file);
        }
      } else {
        System.out.println("No files found in the directory.");
      }
    } else {
      System.err.println("Error: Directory not found or not a directory: " + directoryPath);
    }
  }
}