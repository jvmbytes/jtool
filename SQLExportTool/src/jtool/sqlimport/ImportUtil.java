/**
 * Created By: Comwave Project Team Created Date: 2015年7月24日
 */
package jtool.sqlimport;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class ImportUtil {
    private static final String NUMBER_PATTER = "\\d+(\\.\\d+)?";

    private static final String INT_PATTERN = "\\d+";

    static String dateFormatPattern = "yyyy/MM/dd";

    static SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern);

    static String datetimeFormatPattern = "yyyy/MM/dd HH:mm:ss";

    static SimpleDateFormat datetimeFormat = new SimpleDateFormat(datetimeFormatPattern);

    public static Map<String, Column> getColumnTypeMap(Connection connection, String userName, String tableName) throws SQLException {
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

    public static void addColumnValue(StringBuffer buffer, int rowIndex, String cname, String cvalue, Column column) throws SQLException {
        if (column == null || cname == null) {
            throw new RuntimeException("Error Row[" + rowIndex + "] can't find column:" + cname);
        }

        if (cvalue == null || "".equals(cvalue)) {
            if (!column.isNullable()) {
                throw new SQLException("Error Row[" + rowIndex + "] the value of column[" + cname + "] should not be null!");
            }
            buffer.append("null");
            return;
        }

        if (cvalue.contains("'")) {
            System.err.println("Error Row[" + rowIndex + "] contains single quotation marks in column[" + cname + "]:" + cvalue);
            cvalue = cvalue.replaceAll("'", "\"");
            System.err.println("Replaced with double quotation:" + cvalue);
        }

        String type = column.getType();

        if ("NUMBER".equals(column) || "INT".equals(column)) {
            if (cvalue.length() > column.size) {
                throw new SQLException("Error Row[" + rowIndex + "] the size of the value[" + cvalue + "] of column[" + cname + "] is " + cvalue.length() + ", while the max size is " + column.size);
            }
            if ("NUMBER".equals(column) && !cvalue.matches(NUMBER_PATTER)) {
                throw new SQLException("Error Row[" + rowIndex + "] the value[" + cvalue + "] is not a number!");
            }
            if ("IN".equals(column) && !cvalue.matches(INT_PATTERN)) {
                throw new SQLException("Error Row[" + rowIndex + "] the value[" + cvalue + "] is not a integer!");
            }

            buffer.append(cvalue);
        }
        /* CHAR,VARCHAR,NVARCHAR */
        else if (type.indexOf("CHAR") != -1) {
            if (cvalue.length() > column.size) {
                throw new SQLException("Error Row[" + rowIndex + "] the length of the value[" + cvalue + "] of column[" + cname + "] is " + cvalue.length() + ", while the max length is "
                        + column.size);
            }
            buffer.append("\'" + cvalue + "\'");
        } else if (type.indexOf("TIMESTAMP") != -1) {
            try {
                datetimeFormat.parse(cvalue);
            } catch (ParseException e) {
                throw new RuntimeException("Error Row[" + rowIndex + "] bad date time format value[" + cvalue + "] for column " + cname);
            }
            buffer.append("to_date(\'" + cvalue + "\',\'yyyy/mm/dd hh24:mi:ss\')");
        } else if (type.indexOf("DATE") != -1) {
            try {
                dateFormat.parse(cvalue);
            } catch (ParseException e) {
                throw new RuntimeException("Error Row[" + rowIndex + "] bad date format value[" + cvalue + "] for column " + cname);
            }
            buffer.append("to_date(\'" + cvalue + "\',\'yyyy/mm/dd\')");
        } else {
            buffer.append("\'" + cvalue + "\'");
        }
    }

    public static void executeSqls(Connection connection, List<String> sqls) throws SQLException {
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();

        System.out.println("start execute sql, total count " + sqls.size());
        for (int i = 0; i < sqls.size(); i++) {
            String sql = sqls.get(i);
            try {
                statement.execute(sql);
            } catch (Exception e) {
                System.err.println("error to execute sql[" + i + "]: \t" + sql);
                throw e;
            }
        }

        System.out.println("================================");
        System.out.println("begin commit ...");

        connection.commit();

        System.out.println("finish commit ...");

        statement.close();
    }

    @SuppressWarnings("rawtypes")
    public static String buildInsertSqlPrefix(String tableName, List columns, Map<String, Column> columnTypeMap) throws Exception {
        String insertSqlPrefix = "insert into " + tableName + "(" + columns.get(0);
        for (int i = 1; i < columns.size(); i++) {
            String cname = (String) columns.get(i);
            Column column = columnTypeMap.get(cname);
            if (cname == null || column == null) {
                throw new Exception("No column named " + cname + " in table " + tableName);
            }
            insertSqlPrefix += "," + cname;
        }
        insertSqlPrefix += ") values(";
        return insertSqlPrefix;
    }
}
