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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class ImportUtil {

    private static final Logger logger = LoggerFactory.getLogger(ImportUtil.class);

    private static final String NUMBER_PATTER = "\\d+(\\.\\d+)?";

    private static final String INT_PATTERN = "\\d+";

    static String dateFormatPattern = "yyyy/MM/dd";

    static SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern);

    static String datetimeFormatPattern = "yyyy/MM/dd HH:mm:ss";

    static SimpleDateFormat datetimeFormat = new SimpleDateFormat(datetimeFormatPattern);

    public static Map<String, Column> getColumnMap(Connection connection, String userName, String tableName) throws SQLException {
        Map<String, Column> columnMap = new HashMap<String, Column>();
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

            columnMap.put(name, column);
        }
        logger.info("column count of table " + tableName + ":" + columnMap.size());

        return columnMap;
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
            logger.error("Error Row[" + rowIndex + "] contains single quotation marks in column[" + cname + "]:" + cvalue);
            cvalue = cvalue.replaceAll("'", "\"");
            logger.error("Replaced with double quotation:" + cvalue);
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
        int size = sqls.size();
        logger.info("start execute sql, total size " + size);
        for (int i = 0; i < size; i++) {
            String sql = sqls.get(i);
            try {
                if (i % 1000 == 0) {
                    logger.info("executing progress:" + i + " / " + size);
                }
                statement.execute(sql);
            } catch (Exception e) {
                logger.error("error to execute sql[" + i + "]: \t" + sql);
                if (hasPkFkError(e.getMessage()) && ImportGlobals.isContinueWhenPkFkError()) {
                    logger.error(e.getMessage());
                    continue;
                }
                throw e;
            }
        }

        logger.info("executing progress:" + size + " / " + size);

        logger.info("================================");
        logger.info("begin commit ...");

        connection.commit();

        logger.info("finish commit ...");

        statement.close();
    }

    /**
     * ORA-00001: 违反唯一约束条件 (.) <br>
     * ORA-02261: 表中已存在这样的唯一关键字或主键<br>
     */
    private static boolean hasPkFkError(String message) {
        return message.contains("ORA-02261") || message.contains("ORA-00001");
    }

    @SuppressWarnings("rawtypes")
    public static String buildInsertSqlPrefix(String tableName, List columns, Map<String, Column> columnMap) throws Exception {
        String cname = (String) columns.get(0);
        assertColumnExist(columnMap, tableName, cname);
        String insertSqlPrefix = "insert into " + tableName + "(" + cname;
        for (int i = 1; i < columns.size(); i++) {
            cname = (String) columns.get(i);
            assertColumnExist(columnMap, tableName, cname);
            insertSqlPrefix += "," + cname;
        }
        insertSqlPrefix += ") values(";
        return insertSqlPrefix;
    }

    private static void assertColumnExist(Map<String, Column> columnMap, String tableName, String cname) throws Exception {
        Column column = columnMap.get(cname);
        if (cname == null || column == null) {
            throw new Exception("No column named " + cname + " in table " + tableName);
        }
    }
}
