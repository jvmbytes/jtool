package jtool.sql.exp;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import jtool.sql.domain.Column;
import jtool.sql.util.JdbcUtil;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Geln Yang
 * @createdDate 2014年11月21日
 */
public class SqlExportUtil {
  private static final Logger logger = LoggerFactory.getLogger(SqlExportUtil.class);

  public static StringBuffer exportCSV(ResultSetMetaData rmeta, ResultSet rs,
      Map<String, Column> columnMap) throws SQLException, IOException {
    int numColumns = rmeta.getColumnCount();
    logger.info("column count: " + numColumns);
    logger.info("-------------------------------------");
    String columnNames = "";
    for (int i = 1; i <= numColumns; i++) {
      if (i < numColumns) {
        columnNames += rmeta.getColumnName(i) + " , ";
      } else {
        columnNames += rmeta.getColumnName(i);
      }
    }
    logger.info(columnNames);
    logger.info("-------------------------------------");

    String head = "";
    for (int i = 1; i <= numColumns; i++) {
      head += rmeta.getColumnName(i) + ",";
    }
    head = head.substring(0, head.length() - 1) + "\r\n";

    // output data
    StringBuffer buffer = new StringBuffer();
    buffer.append(head);
    int count = addExportRecords(buffer, columnMap, rmeta, rs, false, "");
    logger.info("Record count: " + count);
    return buffer;
  }

  public static StringBuffer exportInsert(ResultSetMetaData rmeta, ResultSet rs, String tableName,
      Map<String, Column> columnMap) throws SQLException, IOException {
    int numColumns = rmeta.getColumnCount();
    logger.info("column count: " + numColumns);
    logger.info("-------------------------------------");
    for (int i = 1; i <= numColumns; i++) {
      if (i < numColumns) {
        System.out.print(rmeta.getColumnName(i) + " , ");
      } else {
        logger.info(rmeta.getColumnName(i));
      }

      if (tableName == null || tableName.trim().length() == 0) {
        tableName = rmeta.getTableName(i);
      }
    }
    logger.info("-------------------------------------");
    logger.info("Table Name: " + tableName);

    String insertSqlPrefix = "INSERT INTO " + tableName.toUpperCase() + " (";
    for (int i = 1; i <= numColumns; i++) {
      insertSqlPrefix += rmeta.getColumnName(i) + ",";
    }
    insertSqlPrefix = insertSqlPrefix.substring(0, insertSqlPrefix.length() - 1) + ") VALUES(";

    // output data
    StringBuffer buffer = new StringBuffer();
    int count = addExportRecords(buffer, columnMap, rmeta, rs, true, insertSqlPrefix);
    logger.info("Record count: " + count);
    return buffer;
  }

  private static int addExportRecords(StringBuffer buffer, Map<String, Column> columnMap,
      ResultSetMetaData rmeta, ResultSet rs, boolean isInsert, String insertSqlPrefix)
      throws SQLException {
    int count = 0;
    int numColumns = rmeta.getColumnCount();
    while (rs.next()) {
      count++;
      if (isInsert) {
        buffer.append(insertSqlPrefix);
      }
      /* loop add columns values */
      for (int i = 1; i <= numColumns; i++) {
        String data = JdbcUtil.getData(columnMap, rmeta, rs, i);
        addExportRecordColumnValue(buffer, data, i == numColumns, isInsert);
      }
    }
    return count;
  }

  private static void addExportRecordColumnValue(StringBuffer buffer, String data, boolean isLast,
      boolean isInsert) {
    if (data != null) {
      data = data.trim();
      if (isInsert) {
        data = data.replaceAll("'", "\"");
      } else {
        data = data.replaceAll("\"", "'");
      }
      data = isInsert ? "'" + data + "'" : "\"" + data + "\"";
    } else {
      data = isInsert ? "null" : "";
    }

    if (isLast) {
      buffer.append(data);
      if (isInsert) {
        buffer.append(");");
      }
      buffer.append("\r\n");
    } else {
      buffer.append(data + ",");
    }
  }

  public static void saveToFile(String outputFilePath, StringBuffer buffer) throws IOException {
    logger.info("start to write into file ... ");
    FileUtils.writeStringToFile(new File(outputFilePath), buffer.toString(), "UTF-8");
    logger.info("end to write into file ... ");
  }
}
