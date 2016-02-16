/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sql.exp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;

import jtool.sql.domain.Column;
import jtool.sql.util.JdbcUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class ExportCSV {
  private static final Logger logger = LoggerFactory.getLogger(ExportCSV.class);

  public static void main(String[] args) throws Exception {
    String driverName = args[0];
    String linkUrl = args[1];
    String userName = args[2];
    String password = args[3];
    String tableName = args[4];
    String querySql = args[5];
    String outputFilePath = "./" + tableName + ".csv";

    logger.info(driverName);
    logger.info(linkUrl);
    logger.info(userName);
    logger.info(password);
    logger.info(tableName);
    logger.info(querySql);
    logger.info(outputFilePath);

    Class.forName(driverName).newInstance();
    Connection connection = DriverManager.getConnection(linkUrl, userName, password);
    Statement myStmt = connection.createStatement();
    ResultSet rs = myStmt.executeQuery(querySql);
    ResultSetMetaData rmeta = rs.getMetaData();
    Map<String, Column> columnMap = JdbcUtil.getColumnMap(connection, userName, tableName);
    StringBuffer buffer = SqlExportUtil.exportCSV(rmeta, rs, columnMap);
    SqlExportUtil.saveToFile(outputFilePath, buffer);
    rs.close();
    myStmt.close();
    connection.close();
    logger.info("------------------------");
    logger.info("over");
  }
}
