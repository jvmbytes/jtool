/**
 * Created Date: Dec 29, 2015
 */
package jtool.log.parser;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

/**
 * The Class LogParser.
 */
public class LogParser {

  /** The Constant DEFAULT_ENCODING. */
  private static final String DEFAULT_ENCODING = "UTF-8";

  /** The Constant yyyyMMdd. */
  private static final SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");

  /** The Constant LOG_LINE_REGEX. */
  /* FOR OLD PATTERN */
  private static final String LOG_LINE_REGEX =
      "([^ \r\n]+) ([^ ]+) ([^ ]+)[ ]+\\[(([^\\[\\]]+)|(\\[[^\\[\\]]+\\][^\\[\\]]+))\\] ([^ ]+)[ ]+([^ ]+) ([^\r\n]+)";
  // private static final String LOG_LINE_REGEX =
  // "([^ \r\n]+) ([^ ]+)[ ]+\\[(([^\\[\\]]+)|(\\[[^\\[\\]]+\\][^\\[\\]]+))\\] ([^ ]+)[ ]+([^ ]+) ([^\r\n]+)";

  /** The Constant REGEX_GROUP_DATE. */
  private static final int REGEX_GROUP_DATE = 1;

  /** The Constant REGEX_GROUP_TIME. */
  private static final int REGEX_GROUP_TIME = 2;

  /** The Constant REGEX_GROUP_LEVEL. */
  private static final int REGEX_GROUP_LEVEL = 7;

  /** The Constant REGEX_GROUP_MESSAGE. */
  private static final int REGEX_GROUP_MESSAGE = REGEX_GROUP_LEVEL + 2;

  /** The Constant LOGGER_LEVEL_ERROR. */
  private static final Object LOGGER_LEVEL_ERROR = "ERROR";

  /** The Constant MAX_FILE_NAME_LENGTH. */
  private static final int MAX_FILE_NAME_LENGTH = 120;

  /** The Constant SYSTEM_TYPE. */
  private static final String SYSTEM_TYPE = "AP";


  /** The error record no map. */
  private static Map<String, Integer> errorRecordNoMap = Collections
      .synchronizedMap(new HashMap<String, Integer>());

  /**
   * Builds the error record no.
   * 
   * @param systemType the system type
   * @param date the date
   * @param time the time
   * @return the string
   */
  private static synchronized String buildErrorRecordNo(String systemType, String date, String time) {
    String recordNoKey = date.replaceAll("-", "") + "_" + SYSTEM_TYPE;
    Integer lastRecordNo = errorRecordNoMap.get(recordNoKey);
    if (lastRecordNo == null) {
      lastRecordNo = 0;
    }
    lastRecordNo++;
    errorRecordNoMap.put(recordNoKey, lastRecordNo);

    String no = "0000" + lastRecordNo;
    return recordNoKey + "_" + no.substring(no.length() - 4);
  }

  /**
   * the pattern of log file content should be:%date [%thread] %-5level %logger{36} - %msg%n.
   * 
   * @param logFile the log file
   * @param outputDir the output dir
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void extractErrorList(File logFile, File outputDir) throws IOException {
    System.out.println("read file content:" + logFile.getAbsolutePath());
    String content = FileUtils.readFileToString(logFile, DEFAULT_ENCODING);
    Pattern pattern = Pattern.compile(LOG_LINE_REGEX);
    Matcher matcher = pattern.matcher(content);

    Set<String> errorTypeSet = new HashSet<String>();
    List<String> newErrorList = new ArrayList<String>();
    Map<String, String> newErrorMap = new HashMap<String, String>();
    boolean found = matcher.find();
    int start = 0;
    int end = 0;
    String logLevel;
    String errorTitle;
    String errorMessage;
    String date;
    String time;
    while (found) {
      logLevel = matcher.group(REGEX_GROUP_LEVEL);
      if (LOGGER_LEVEL_ERROR.equals(logLevel)) {
        errorTitle = matcher.group(REGEX_GROUP_MESSAGE);

        if (errorTypeSet.contains(errorTitle)) {
          found = matcher.find();
          continue;
        }
        errorTypeSet.add(errorTitle);

        date = matcher.group(REGEX_GROUP_DATE);
        time = matcher.group(REGEX_GROUP_TIME);
        String errorRecordNo = buildErrorRecordNo(SYSTEM_TYPE, date, time);
        newErrorList.add(errorRecordNo);
        newErrorMap.put(errorRecordNo, errorTitle);

        start = matcher.start();
        found = matcher.find();

        end = found ? matcher.start() : content.length();
        errorMessage = content.substring(start, end);

        outputError(outputDir, date, time, errorRecordNo, errorTitle, errorMessage);
      } else {
        found = matcher.find();
      }
    }

    outputErrorList(outputDir, newErrorList, newErrorMap);
  }

  /**
   * Output error list.
   * 
   * @param outputDir the output dir
   * @param newErrorList the new error list
   * @param newErrorMap the new error map
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void outputErrorList(File outputDir, List<String> newErrorList,
      Map<String, String> newErrorMap) throws IOException {
    String date = yyyyMMdd.format(new Date());
    StringBuffer buffer = new StringBuffer();
    buffer.append("<html><title>" + date + " new error list</title><body>");
    buffer.append("<table border='1'>");
    buffer.append("<tr><th>Record No</th><th>Error Message</th></tr>");
    for (int i = 0; i < newErrorList.size(); i++) {
      String errorRecordNo = newErrorList.get(i);
      String error = newErrorMap.get(errorRecordNo);
      buffer.append("<tr><td>" + errorRecordNo + "</td><td>" + error + "</td></tr>");
    }
    buffer.append("</table>");
    buffer.append("</body></html>");

    String errorListFilePath =
        outputDir.getAbsolutePath() + File.separator + date + "_new_error_list.html";
    System.out.println("write error list:" + errorListFilePath);
    FileUtils.writeStringToFile(new File(errorListFilePath), buffer.toString(), DEFAULT_ENCODING);
  }

  /**
   * Output error.
   * 
   * @param outputDir the output dir
   * @param date the date
   * @param time the time
   * @param errorRecordNo the error record no
   * @param errorTitle the error title
   * @param errorMessage the error message
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void outputError(File outputDir, String date, String time, String errorRecordNo,
      String errorTitle, String errorMessage) throws IOException {
    String logDirPath = outputDir.getAbsolutePath() + File.separator + date;
    FileUtils.forceMkdir(new File(logDirPath));

    // String fileName = errorTitle;
    // fileName = fileName.replaceAll("[:,\\W]", "_").replaceAll("__", "_").replaceAll("__", "_");
    // fileName = errorRecordNo + "_" + fileName;
    String fileName = errorRecordNo;
    if (fileName.length() > MAX_FILE_NAME_LENGTH) {
      fileName = fileName.substring(0, MAX_FILE_NAME_LENGTH);
    }
    System.out.println("------------------------------------------");
    System.out.println(date + " " + time + " " + errorTitle);
    String filePath = logDirPath + File.separator + fileName + ".log";
    FileUtils.writeStringToFile(new File(filePath), errorMessage, DEFAULT_ENCODING);
  }
}
