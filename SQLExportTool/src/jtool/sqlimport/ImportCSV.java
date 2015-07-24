/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sqlimport;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;

/**
 * It's just for oracle right now!
 * 
 * @author Geln Yang
 * @version 1.0
 */
public class ImportCSV {

    public static void main(String[] args) throws Exception {
        String driverName = args[0];
        String linkUrl = args[1];
        String userName = args[2];
        String password = args[3];
        String tableName = args[4];
        String cvsFilePath = args[5];
        String fileEncode = args[6];
        if (StringUtils.isBlank(fileEncode)) {
            fileEncode = "UTF-8";
        }

        System.out.println("----------------------");
        System.out.println(new Date());
        System.out.println(driverName);
        System.out.println(linkUrl);
        System.out.println(userName);
        System.out.println(password);
        System.out.println(tableName);
        System.out.println(cvsFilePath);

        imp(driverName, linkUrl, userName, password, tableName, cvsFilePath, fileEncode);

        System.out.println("------------------------");
        System.out.println("over");
    }

    @SuppressWarnings("resource")
    public static void imp(String driverName, String linkUrl, String userName, String password, String tableName, String cvsFilePath, String fileEncode) throws Exception {
        Class.forName(driverName).newInstance();
        Connection connection = DriverManager.getConnection(linkUrl, userName, password);

        Map<String, Column> columnTypeMap = ImportUtil.getColumnTypeMap(connection, userName, tableName);

        File file = new File(cvsFilePath);
        Reader reader = new InputStreamReader(new FileInputStream(file), fileEncode);
        CSVParser parser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());
        Map<String, Integer> headerMap = parser.getHeaderMap();
        ArrayList<String> columns = new ArrayList<String>();
        columns.addAll(headerMap.keySet());

        String insertSqlPrefix = ImportUtil.buildInsertSqlPrefix(tableName, columns, columnTypeMap);

        List<CSVRecord> records = parser.getRecords();
        List<String> sqls = new ArrayList<String>();
        for (int recordIndex = 0; recordIndex < records.size(); recordIndex++) {
            CSVRecord csvRecord = records.get(recordIndex);
            String cname = columns.get(0);
            String cvalue = csvRecord.get(0);
            Column column = columnTypeMap.get(cname);

            StringBuffer sqlBuffer = new StringBuffer();
            sqlBuffer.append(insertSqlPrefix);
            ImportUtil.addColumnValue(sqlBuffer, recordIndex, cname, cvalue, column); // add
                                                                                      // first

            for (int j = 1; j < columns.size(); j++) {
                cname = columns.get(j);
                cvalue = csvRecord.get(j);
                column = columnTypeMap.get(cname);
                sqlBuffer.append(",");
                ImportUtil.addColumnValue(sqlBuffer, recordIndex, cname, cvalue, column);
            }
            sqlBuffer.append(")");
            String sql = sqlBuffer.toString();

            sqls.add(sql);
        }

        ImportUtil.executeSqls(connection, sqls);
    }

}
