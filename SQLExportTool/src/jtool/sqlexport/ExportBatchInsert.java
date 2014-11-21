/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sqlexport;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.io.FileUtils;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class ExportBatchInsert {

    public static void main(String[] args) throws Exception {
        String driverName = args[0];
        String linkUrl = args[1];
        String userName = args[2];
        String password = args[3];
        String saveFileName = args[4];
        String batchSql = args[5];
        String batchSqlFilePath = "./" + batchSql;
        String outputFilePath = "./" + saveFileName + ".sql";

        System.out.println(driverName);
        System.out.println(linkUrl);
        System.out.println(userName);
        System.out.println(password);
        System.out.println(batchSql);
        System.out.println(outputFilePath);

        Class.forName(driverName).newInstance();
        Connection myConn = DriverManager.getConnection(linkUrl, userName, password);

        StringBuffer buffer = new StringBuffer();
        List<String> lines = FileUtils.readLines(new File(batchSqlFilePath), ExportConstants.DEFAULT_ENCODE);

        if (lines != null) {
            for (String sql : lines) {
                Statement myStmt = myConn.createStatement();
                ResultSet rs = myStmt.executeQuery(sql);
                ResultSetMetaData rmeta = rs.getMetaData();

                String sqlString = sql.toUpperCase();
                int fromIndex = sqlString.indexOf("FROM");
                sqlString = sqlString.substring(fromIndex + 4).trim();
                int blankIndex = sqlString.indexOf(" ");
                String tableName = sqlString.substring(0, blankIndex);

                StringBuffer result = SqlExportUtil.exportInsert(rmeta, rs, tableName);
                buffer.append("-- " + tableName + ExportConstants.NEW_LINE);
                buffer.append(result + ExportConstants.NEW_LINE);
                rs.close();
                myStmt.close();
            }
        }

        SqlExportUtil.saveToFile(outputFilePath, buffer);

        myConn.close();
        System.out.println("------------------------");
        System.out.println("over");
    }
}
