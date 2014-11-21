/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sqlexport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class ExportInsert {

    public static void main(String[] args) throws Exception {
        String driverName = args[0];
        String linkUrl = args[1];
        String userName = args[2];
        String password = args[3];
        String tableName = args[4];
        String saveFileName = args[5];
        String querySql = args[6];
        String outputFilePath = "./" + saveFileName + ".sql";

        System.out.println(driverName);
        System.out.println(linkUrl);
        System.out.println(userName);
        System.out.println(password);
        System.out.println(tableName);
        System.out.println(querySql);
        System.out.println(outputFilePath);

        Class.forName(driverName).newInstance();
        Connection myConn = DriverManager.getConnection(linkUrl, userName, password);
        Statement myStmt = myConn.createStatement();
        ResultSet rs = myStmt.executeQuery(querySql);
        ResultSetMetaData rmeta = rs.getMetaData();

        StringBuffer buffer = SqlExportUtil.exportInsert(rmeta, rs, tableName);
        SqlExportUtil.saveToFile(outputFilePath, buffer);

        rs.close();
        myStmt.close();
        myConn.close();
        System.out.println("------------------------");
        System.out.println("over");
    }
}
