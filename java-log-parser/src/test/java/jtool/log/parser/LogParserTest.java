/**
 * Created Date: Dec 29, 2015
 */
package jtool.log.parser;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * Unit test for simple App.
 */
public class LogParserTest

{

  /**
   * The main method.
   * 
   * @param args the arguments
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void main(String[] args) throws IOException {
    File logFile = new File("C:\\tmp\\bpm.log");
    File outputFile = new File("C:\\tmp\\output");
    if (!outputFile.exists()) {
      FileUtils.forceMkdir(outputFile);
    }
    LogParser.extractErrorList(logFile, outputFile);
  }
}
