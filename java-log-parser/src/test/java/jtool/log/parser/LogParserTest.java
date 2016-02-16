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
    extractLog("sale");
    extractLog("org");
    extractLog("report");
    extractLog("sale");
    extractLog("settlement");
  }

  private static void extractLog(String logName) throws IOException {
    File logFile = new File("C:\\tmp\\"+logName+".log");
    File outputFile = new File("C:\\tmp\\output\\"+logName);
    if (!outputFile.exists()) {
      FileUtils.forceMkdir(outputFile);
    }
    LogParser.extractErrorList(logFile, outputFile);
    
  }
}
