package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.logging.Logger;

public class FileUtility {

  private static final Logger LOGGER = Logger.getLogger(FileUtility.class.getName());

  public static void createDataDir(String dataDir) {
    File file = new File(dataDir);
    if (!file.exists()) {
      file.mkdir();
    }
  }

  /**
   * @param isFirstLines If first line of the file has been written We need this
   *                     to know if subsequent lines in the file needs a new line
   * @param lines        Lines to be written
   * @param writter      The file's bufferwritter
   * @return returns if the first line of the file has been written
   */
  public static boolean writeToFile(boolean isFirstLines, List<String> lines, BufferedWriter writter) {
    try {
      for (String line : lines) {
        if (isFirstLines) {
          isFirstLines = false;
        } else {
          writter.newLine();
        }
        writter.write(line);
      }
      writter.flush();
    } catch (IOException e) {
      LOGGER.severe("Unable to write to file " + e.getMessage());
    }
    return isFirstLines;
  }

  public static BufferedReader getReader(String fileName) throws FileNotFoundException {
    return new BufferedReader(new FileReader(fileName));
  }

  public static BufferedWriter getWritter(String fileName) {
    try {
      return new BufferedWriter(new FileWriter(fileName, true));
    } catch (IOException e) {
      LOGGER.severe("Unable to setup message logs " + e.getMessage());
      return null;
    }
  }

  public static void serialize(Object object, String filaname) {
    try {
      ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(filaname));
      stream.writeObject(object);
      stream.close();
    } catch (IOException e) {
      LOGGER.severe("Unable to persist topic mappings " + e.getMessage());
    }
  }

  public static Object deserialize(String topicsDataFile) {
    Object object = null;
    try {
      ObjectInputStream stream = new ObjectInputStream(new FileInputStream(topicsDataFile));
      object =  stream.readObject();
      stream.close();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.severe("Unable to load topic mappings " + e.getMessage());
    }
    return object;
  }

  public static void cleanData() {
    LOGGER.info("Test directory --- " + new File("data").getAbsolutePath());
    // Remove all test data
    try {
      for (File file : new File("data").listFiles()) {
        if (!file.getName().startsWith(".")) {
          file.delete();
        }
      }
    } catch (Exception e) {
      LOGGER.severe("Unable to clean " + new File("data").getAbsolutePath());
    }
  }
}