/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sqlimport;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
    static String dateFormatPattern = "yyyy/MM/dd";

    static SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern);

    static String datetimeFormatPattern = "yyyy/MM/dd HH:mm:ss";

    static SimpleDateFormat datetimeFormat = new SimpleDateFormat(datetimeFormatPattern);

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

        Class.forName(driverName).newInstance();
        Connection connection = DriverManager.getConnection(linkUrl, userName, password);
        DatabaseMetaData metaData = connection.getMetaData();

        Map<String, String> columnTypeMap = new HashMap<String, String>();

        String columnName;
        String columnType;
        ResultSet columnResultSet = metaData.getColumns(null, userName, tableName, "%");
        while (columnResultSet.next()) {
            columnName = columnResultSet.getString("COLUMN_NAME");
            columnType = columnResultSet.getString("TYPE_NAME");
            // int datasize = columnResultSet.getInt("COLUMN_SIZE");
            // int digits = columnResultSet.getInt("DECIMAL_DIGITS");
            // int nullable = columnResultSet.getInt("NULLABLE");
            columnTypeMap.put(columnName, columnType);
        }
        System.out.println("column count of table " + tableName + ":" + columnTypeMap.size());

        File file = new File(cvsFilePath);
        Reader reader = new InputStreamReader(new FileInputStream(file), fileEncode);
        CSVParser parser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());
        Map<String, Integer> headerMap = parser.getHeaderMap();
        ArrayList<String> columns = new ArrayList<String>();
        columns.addAll(headerMap.keySet());

        String insertSqlPrefix = "insert into " + tableName + "(" + columns.get(0);
        for (int i = 1; i < columns.size(); i++) {
            insertSqlPrefix += "," + columns.get(i);
        }
        insertSqlPrefix += ") values(";

        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        List<CSVRecord> records = parser.getRecords();
        for (CSVRecord csvRecord : records) {
            String cname = columns.get(0);
            String cvalue = csvRecord.get(0);
            String ctype = columnTypeMap.get(cname);

            StringBuffer sqlBuffer = new StringBuffer();
            sqlBuffer.append(insertSqlPrefix);
            addColumnValue(sqlBuffer, cname, cvalue, ctype); // add first

            for (int i = 1; i < columns.size(); i++) {
                cname = columns.get(i);
                cvalue = csvRecord.get(i);
                ctype = columnTypeMap.get(cname);
                sqlBuffer.append(",");
                addColumnValue(sqlBuffer, cname, cvalue, ctype);
            }
            sqlBuffer.append(")");
            String sql = sqlBuffer.toString();
            System.out.println(sql);
            statement.execute(sql);
        }

        parser.close();

        System.out.println("================================");
        System.out.println("begin commit ...");

        connection.commit();

        System.out.println("finish commit ...");

        statement.close();
        connection.close();
        System.out.println("------------------------");
        System.out.println("over");
    }

    private static void addColumnValue(StringBuffer buffer, String cname, String cvalue, String ctype) {
        if (ctype == null || cname == null) {
            throw new RuntimeException("can't find column:" + cname);
        }

        if (cvalue == null || cvalue.equals("")) {
            buffer.append("null");
            return;
        }

        if ("NUMBER".equals(ctype) || "INT".equals(ctype)) {
            buffer.append(cvalue);
        } else if (ctype.indexOf("CHAR") != -1) {
            buffer.append("\'" + cvalue + "\'");
        } else if (ctype.indexOf("TIMESTAMP") != -1) {
            try {
                datetimeFormat.parse(cvalue);
            } catch (ParseException e) {
                throw new RuntimeException("bad date time format value[" + cvalue + "] for column " + cname);
            }
            buffer.append("to_date(\'" + cvalue + "\',\'yyyy/mm/dd hh24:mi:ss\')");
        } else if (ctype.indexOf("DATE") != -1) {
            try {
                dateFormat.parse(cvalue);
            } catch (ParseException e) {
                throw new RuntimeException("bad date format value[" + cvalue + "] for column " + cname);
            }
            buffer.append("to_date(\'" + cvalue + "\',\'yyyy/mm/dd\')");
        } else {
            buffer.append("\'" + cvalue + "\'");
        }
    }
}
