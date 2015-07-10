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
import java.sql.SQLException;
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
    private static final String NUMBER_PATTER = "\\d+(\\.\\d+)?";

    private static final String INT_PATTERN = "\\d+";

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

        Map<String, Column> columnTypeMap = getColumnTypeMap(connection, userName, tableName);

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

        List<CSVRecord> records = parser.getRecords();
        List<String> sqls = new ArrayList<String>();
        for (int recordIndex = 0; recordIndex < records.size(); recordIndex++) {
            CSVRecord csvRecord = records.get(recordIndex);
            String cname = columns.get(0);
            String cvalue = csvRecord.get(0);
            Column column = columnTypeMap.get(cname);

            StringBuffer sqlBuffer = new StringBuffer();
            sqlBuffer.append(insertSqlPrefix);
            addColumnValue(sqlBuffer, recordIndex, cname, cvalue, column); // add first

            for (int j = 1; j < columns.size(); j++) {
                cname = columns.get(j);
                cvalue = csvRecord.get(j);
                column = columnTypeMap.get(cname);
                sqlBuffer.append(",");
                addColumnValue(sqlBuffer, recordIndex, cname, cvalue, column);
            }
            sqlBuffer.append(")");
            String sql = sqlBuffer.toString();

            sqls.add(sql);
        }
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        for (int i = 0; i < sqls.size(); i++) {
            String sql = sqls.get(i);
            System.out.println(i + "\t : \t" + sql);
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

    private static Map<String, Column> getColumnTypeMap(Connection connection, String userName, String tableName) throws SQLException {
        Map<String, Column> columnTypeMap = new HashMap<String, Column>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columnResultSet = metaData.getColumns(null, userName.toUpperCase(), tableName.toUpperCase(), "%");

        while (columnResultSet.next()) {
            String name = columnResultSet.getString("COLUMN_NAME");
            String type = columnResultSet.getString("TYPE_NAME");
            int size = columnResultSet.getInt("COLUMN_SIZE");
            int digits = columnResultSet.getInt("DECIMAL_DIGITS");
            int nullable = columnResultSet.getInt("NULLABLE");

            Column column = new Column();
            column.setName(name);
            column.setType(type);
            column.setSize(size);
            column.setDecimalDigits(digits);
            column.setNullable(nullable > 0);

            if (name.equals("ACHV_STATUS")) {
                System.out.println("");
            }
            columnTypeMap.put(name, column);
        }
        System.out.println("column count of table " + tableName + ":" + columnTypeMap.size());

        return columnTypeMap;
    }

    private static void addColumnValue(StringBuffer buffer, int columnIndex, String cname, String cvalue, Column column) throws SQLException {
        if (column == null || cname == null) {
            throw new RuntimeException("Error column[" + columnIndex + "] can't find column:" + cname);
        }

        if (cvalue == null || "".equals(cvalue)) {
            if (!column.isNullable()) {
                throw new SQLException("Error column[" + columnIndex + "] the value of column[" + cname + "] should not be null!");
            }
            buffer.append("null");
            return;
        }

        if (cvalue.contains("'")) {
            throw new SQLException("Error column[" + columnIndex + "] contains single quotation marks in column[" + cname + "]:" + cvalue);
        }

        String type = column.getType();

        if ("NUMBER".equals(column) || "INT".equals(column)) {
            if (cvalue.length() > column.size) {
                throw new SQLException("Error column[" + columnIndex + "] the size of column[" + cname + "][" + cvalue + "] is " + cvalue.length() + ", while the max size is " + column.size);
            }
            if ("NUMBER".equals(column) && !cvalue.matches(NUMBER_PATTER)) {
                throw new SQLException("Error column[" + columnIndex + "][" + cvalue + "] is not a number!");
            }
            if ("IN".equals(column) && !cvalue.matches(INT_PATTERN)) {
                throw new SQLException("Error column[" + columnIndex + "][" + cvalue + "] is not a integer!");
            }

            buffer.append(cvalue);
        }
        /* CHAR,VARCHAR,NVARCHAR */
        else if (type.indexOf("CHAR") != -1) {
            if (cvalue.length() > column.size) {
                throw new SQLException("Error column[" + columnIndex + "] the length of column[" + cname + "][" + cvalue + "] is " + cvalue.length() + ", while the max length is " + column.size);
            }
            buffer.append("\'" + cvalue + "\'");
        } else if (type.indexOf("TIMESTAMP") != -1) {
            try {
                datetimeFormat.parse(cvalue);
            } catch (ParseException e) {
                throw new RuntimeException("bad date time format value[" + cvalue + "] for column " + cname);
            }
            buffer.append("to_date(\'" + cvalue + "\',\'yyyy/mm/dd hh24:mi:ss\')");
        } else if (type.indexOf("DATE") != -1) {
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
