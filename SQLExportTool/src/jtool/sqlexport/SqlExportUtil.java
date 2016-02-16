package jtool.sqlexport;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import jtool.sql.domain.Column;
import jtool.sql.util.JdbcUtil;

import org.apache.commons.io.FileUtils;

/**
 * @author Geln Yang
 * @createdDate 2014年11月21日
 */
public class SqlExportUtil {

  public static StringBuffer exportCSV(ResultSetMetaData rmeta, ResultSet rs,
      Map<String, Column> columnMap) throws SQLException, IOException {
    int numColumns = rmeta.getColumnCount();
    System.out.println("column count: " + numColumns);
    System.out.println("-------------------------------------");
    for (int i = 1; i <= numColumns; i++) {
      if (i < numColumns) {
        System.out.print(rmeta.getColumnName(i) + " , ");
      } else {
        System.out.println(rmeta.getColumnName(i));
      }
    }
    System.out.println("-------------------------------------");

    String head = "";
    for (int i = 1; i <= numColumns; i++) {
      head += rmeta.getColumnName(i) + ",";
    }
    head = head.substring(0, head.length() - 1) + "\r\n";

    // output data
    StringBuffer buffer = new StringBuffer();
    buffer.append(head);
    int count = 0;
    while (rs.next()) {
      count++;
      for (int i = 1; i <= numColumns; i++) {
        String data = JdbcUtil.getData(columnMap, rmeta, rs, i);
        if (data != null) {
          if (i < numColumns) {
            buffer.append("\"" + data.trim() + "\",");
          } else {
            buffer.append("\"" + rs.getString(i).trim() + "\"\r\n");
          }
        } else {
          if (i < numColumns) {
            buffer.append(",");
          } else {
            buffer.append("\r\n");
          }
        }
      }
    }

    System.out.println("Record count: " + count);
    return buffer;
  }

  public static StringBuffer exportInsert(ResultSetMetaData rmeta, ResultSet rs, String tableName,
      Map<String, Column> columnMap) throws SQLException, IOException {
    int numColumns = rmeta.getColumnCount();
    System.out.println("column count: " + numColumns);
    System.out.println("-------------------------------------");
    for (int i = 1; i <= numColumns; i++) {
      if (i < numColumns) {
        System.out.print(rmeta.getColumnName(i) + " , ");
      } else {
        System.out.println(rmeta.getColumnName(i));
      }

      if (tableName == null || tableName.trim().length() == 0) {
        tableName = rmeta.getTableName(i);
      }
    }
    System.out.println("-------------------------------------");
    System.out.println("Table Name: " + tableName);

    String insertSqlPrefix = "INSERT INTO " + tableName.toUpperCase() + " (";
    for (int i = 1; i <= numColumns; i++) {
      insertSqlPrefix += rmeta.getColumnName(i) + ",";
    }
    insertSqlPrefix = insertSqlPrefix.substring(0, insertSqlPrefix.length() - 1) + ") VALUES(";

    // output data
    StringBuffer buffer = new StringBuffer();
    int count = 0;
    while (rs.next()) {
      count++;
      buffer.append(insertSqlPrefix);
      for (int i = 1; i <= numColumns; i++) {
        String data = JdbcUtil.getData(columnMap, rmeta, rs, i);
        if (data != null) {
          if (i < numColumns) {
            buffer.append("'" + data.trim() + "',");
          } else {
            buffer.append("'" + rs.getString(i).trim() + "');\r\n");
          }
        } else {
          if (i < numColumns) {
            buffer.append(",");
          } else {
            buffer.append(");\r\n");
          }
        }
      }
    }

    System.out.println("Record count: " + count);
    return buffer;
  }

  public static void saveToFile(String outputFilePath, StringBuffer buffer) throws IOException {
    System.out.println("start to write into file ... ");
    FileUtils.writeStringToFile(new File(outputFilePath), buffer.toString(), "UTF-8");
    System.out.println("end to write into file ... ");
  }
}
