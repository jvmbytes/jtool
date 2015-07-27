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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jtool.sqlimport.domain.DataHolder;
import jtool.sqlimport.domain.RowHolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class ImportUtil {

    private static final Logger logger = LoggerFactory.getLogger(ImportUtil.class);

    private static final String NUMBER_PATTER = "\\-?\\d+(\\.\\d+)?";

    private static final String INT_PATTERN = "\\d+";

    static String dateFormatPattern = "yyyy/MM/dd";

    static SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern);

    static String datetimeFormatPattern = "yyyy/MM/dd HH:mm:ss";

    static SimpleDateFormat datetimeFormat = new SimpleDateFormat(datetimeFormatPattern);

    public static Set<String> getPrimaryKeys(Connection connection, String userName, String tableName) throws SQLException {
        logger.info("start to get table primary keys ...");
        Set<String> columnSet = new HashSet<String>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys(null, userName.toUpperCase(), tableName.toUpperCase());
        while (rs.next()) {
            String name = rs.getString("COLUMN_NAME");
            columnSet.add(name);
        }
        logger.info("finish to get table primary keys:" + columnSet);

        return columnSet;
    }

    public static Map<String, Column> getColumnMap(Connection connection, String userName, String tableName) throws SQLException {
        logger.info("start to get table column definitions ...");
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
        logger.info("finish to get table column definitions ...");
        logger.info("column count of table " + tableName + ":" + columnMap.size());

        return columnMap;
    }

    public static void addColumnValue(StringBuffer buffer, int rowIndex, String cname, String cvalue, Column column) throws Exception {
        Object formatedValue = getSQLFormatedValue(rowIndex, cname, cvalue, column, false);
        buffer.append(formatedValue.toString());
    }

    public static Object getSQLFormatedValue(int rowIndex, String cname, String cvalue, Column column, boolean bulkinsert) throws Exception {
        if (column == null || cname == null) {
            throw new RuntimeException("Error Row[" + rowIndex + "] can't find column:" + cname);
        }

        String type = column.getType();

        if (cvalue == null || cvalue.trim().length() == 0) {
            if (cvalue != null && ("NUMBER".equals(type) || "INT".equals(type))) {
                cvalue = cvalue.trim();
            }

            if (cvalue == null || "".equals(cvalue)) {
                if (!column.isNullable()) {
                    throw new SQLException("Error Row[" + rowIndex + "] the value of column[" + cname + "] should not be null!");
                }
                if (bulkinsert) {
                    return null;
                }
                return "null";
            }
        }

        if (!bulkinsert && cvalue.contains("'")) {
            String rvalue = cvalue.replaceAll("'", "\"");
            logger.error("Error Row[" + rowIndex + "] contains single quotation marks in column[" + cname + "], change value from [" + cvalue + "] to [" + rvalue + "]");
            cvalue = rvalue;
        }

        cvalue = cvalue.trim();
        if ((cname.contains("_TEL") || cname.contains("PHONE") || cname.contains("MOBILE")) && cvalue.length() > 0) {
            cvalue = cvalue.replaceAll(" ", "");
            cvalue = cvalue.replaceAll("０", "0");
            cvalue = cvalue.replaceAll("１", "1");
            cvalue = cvalue.replaceAll("２", "2");
            cvalue = cvalue.replaceAll("３", "3");
            cvalue = cvalue.replaceAll("４", "4");
            cvalue = cvalue.replaceAll("５", "5");
            cvalue = cvalue.replaceAll("６", "6");
            cvalue = cvalue.replaceAll("７", "7");
            cvalue = cvalue.replaceAll("８", "8");
            cvalue = cvalue.replaceAll("９", "9");
            cvalue = cvalue.replaceAll(",", "");
            cvalue = cvalue.replaceAll("？", "");
            cvalue = cvalue.replaceAll("`", "");
            cvalue = cvalue.replaceAll("—", "-");
            cvalue = cvalue.replaceAll("——", "-");
            cvalue = cvalue.replaceAll("）", ")");
            cvalue = cvalue.replaceAll("（", "(");

            String rvalue = cvalue.replaceAll("[^0-9\\-\\(\\)]", "");
            if (!rvalue.equals(cvalue)) {
                logger.warn("Error Row[" + rowIndex + "] wrong tel/mobile/phone number format, change it from [" + cvalue + "] to [" + rvalue + "]");
                cvalue = rvalue;
            }
        }

        if ("NUMBER".equals(type) || "INT".equals(type)) {
            String rvalue = cvalue.replaceAll(",", "");
            rvalue = rvalue.replaceAll("百", "00");
            rvalue = rvalue.replaceAll("千", "000");
            rvalue = rvalue.replaceAll("万", "0000");
            if (rvalue.contains("∞")) {
                // String temp = "9999999999999999999999999999999999999";
                // rvalue = rvalue.replaceAll("∞", temp.substring(temp.length()
                // - column.size + column.decimalDigits + 4 + (rvalue.length() -
                // 1)));
                rvalue = "0";
            }
            rvalue = rvalue.replaceAll("[^0-9\\.\\-]", "");

            if (!rvalue.equals(cvalue)) {
                logger.warn("Error Row[" + rowIndex + "] wrong number format, change it from [" + cvalue + "] to [" + rvalue + "]");
                cvalue = rvalue;
            }
            if (ImportGlobals.isChangeNegativeToZero() && cvalue.startsWith("-")) {
                return "0";
            }

            if ("".equals(cvalue)) {
                return null;
            }

            if (cvalue.length() > column.size) {
                throw new SQLException("Error Row[" + rowIndex + "] the size of the value[" + cvalue + "] of column[" + cname + "] is " + cvalue.length() + ", while the max size is " + column.size);
            }
            int dotIndex = cvalue.indexOf(".");
            if (dotIndex == -1) {
                dotIndex = cvalue.length();
            }
            if (dotIndex > column.getIntegerSize()) {
                throw new SQLException("Error Row[" + rowIndex + "] the integer size of the value[" + cvalue + "] of column[" + cname + "] is " + dotIndex + ", while the integer max size is "
                        + column.getIntegerSize());
            }
            if ("NUMBER".equals(column) && !cvalue.matches(NUMBER_PATTER)) {
                throw new SQLException("Error Row[" + rowIndex + "] the value[" + cvalue + "] is not a number!");
            }
            if ("INT".equals(column) && !cvalue.matches(INT_PATTERN)) {
                throw new SQLException("Error Row[" + rowIndex + "] the value[" + cvalue + "] is not a integer!");
            }

            return cvalue;
        }
        /* CHAR,VARCHAR,NVARCHAR */
        else if (type.indexOf("CHAR") != -1) {
            if (cvalue.length() > column.size) {
                throw new SQLException("Error Row[" + rowIndex + "] the length of the value[" + cvalue + "] of column[" + cname + "] is " + cvalue.length() + ", while the max length is "
                        + column.size);
            }
            if (bulkinsert) {
                return cvalue;
            }
            return "\'" + cvalue + "\'";
        } else if (type.indexOf("TIMESTAMP") != -1) {
            try {
                Date date = datetimeFormat.parse(cvalue);
                if (bulkinsert) {
                    return new java.sql.Date(date.getTime());
                }
            } catch (ParseException e) {
                throw new RuntimeException("Error Row[" + rowIndex + "] bad date time format value[" + cvalue + "] for column " + cname);
            }
            return "to_date(\'" + cvalue + "\',\'yyyy/mm/dd hh24:mi:ss\')";
        } else if (type.indexOf("DATE") != -1) {
            try {
                Date date = dateFormat.parse(cvalue);
                if (bulkinsert) {
                    return new java.sql.Date(date.getTime());
                }
            } catch (ParseException e) {
                throw new RuntimeException("Error Row[" + rowIndex + "] bad date format value[" + cvalue + "] for column " + cname);
            }
            return "to_date(\'" + cvalue + "\',\'yyyy/mm/dd\')";
        } else {
            if (bulkinsert) {
                return cvalue;
            }
            return "\'" + cvalue + "\'";
        }
    }

    public static void executeSqls(Connection connection, List<String> sqls) throws SQLException {
        connection.setAutoCommit(ImportGlobals.isAutoCommit());
        Statement statement = connection.createStatement();
        int size = sqls.size();
        int successCount = 0;
        int errorCount = 0;
        logger.info("start execute sql, total size " + size);
        for (int i = 0; i < size; i++) {
            String sql = sqls.get(i);
            try {
                statement.execute(sql);
            } catch (Exception e) {
                logger.error("error to execute sql[" + i + "]: \t" + sql);
                errorCount++;
                if (OracleUtil.hasPkFkError(e.getMessage()) && ImportGlobals.isContinueWhenPkFkError()) {
                    logger.error(e.getMessage());
                    continue;
                }
                throw e;
            }

            if (i % ImportGlobals.getBatchsize() == 0) {
                if (i > 0 && !ImportGlobals.isAutoCommit()) {
                    connection.commit();
                }
                logger.info("executing progress:" + i + " / " + size);
            }

            successCount++;
        }
        if (!ImportGlobals.isAutoCommit()) {
            connection.commit();
        }
        logger.info("executing progress:" + size + " / " + size);
        logger.info("success count:" + successCount + ", error count:" + errorCount);
        logger.info("================================");
        logger.info("finish execute ...");

        statement.close();
    }

    public static void bulkinsetImp(Connection connection, String userName, String tableName, List<String> columns, Map<String, Column> columnMap, DataHolder dataHolder) throws Exception {
        OracleUtil.createBulkInsertProcedure(connection, tableName, columns, columnMap);
        Set<String> primaryKeys = ImportUtil.getPrimaryKeys(connection, userName, tableName);
        Set<String> primaryKeyValueSet = new HashSet<String>();

        int totalSize = dataHolder.getSize();
        logger.info("start to parse data, total data size:" + totalSize);
        int bulkinsertSize = totalSize;
        if (bulkinsertSize > ImportGlobals.getMaxBulksize()) {
            bulkinsertSize = ImportGlobals.getMaxBulksize();
        }
        logger.info("initial bulk insert size:" + bulkinsertSize);
        Object[][] dataArr = new Object[columns.size()][bulkinsertSize];
        int exactDataIndex = 0;
        int bulkinsertCount = 0;
        int errorDataCount = 0;
        for (int recordIndex = 0; recordIndex < dataHolder.getSize(); recordIndex++) {
            RowHolder row = dataHolder.getRow(recordIndex);
            String primaryKeyValue = "";
            for (int i = 0; i < columns.size(); i++) {
                String cname = columns.get(i);
                String cvalue = (String) row.get(i);
                Column column = columnMap.get(cname);

                Object value = ImportUtil.getSQLFormatedValue(recordIndex, cname, cvalue, column, true);
                dataArr[i][bulkinsertCount] = value;

                if (primaryKeys.contains(cname)) {
                    primaryKeyValue += value.toString();
                }
            }

            primaryKeyValue = primaryKeyValue.trim();
            if (primaryKeyValueSet.contains(primaryKeyValue)) {
                String errorMessage = "Row[" + recordIndex + "] duplicated primay key value:" + primaryKeyValue;
                if (ImportGlobals.isContinueWhenPkFkError()) {
                    logger.error(errorMessage);
                    errorDataCount++;
                } else {
                    throw new Exception(errorMessage);
                }
            } else {
                primaryKeyValueSet.add(primaryKeyValue);
                /* increase when no duplicated */
                exactDataIndex++;
                bulkinsertCount++;
            }

            /* reach the data array size */
            if (bulkinsertSize < totalSize && bulkinsertCount == bulkinsertSize) {
                OracleUtil.bulkinsert(connection, tableName, columns, dataArr);
                /* reset data array */
                dataArr = new Object[columns.size()][bulkinsertSize];
                /* reset bulk insert count to zero */
                bulkinsertCount = 0;
            }
        }
        logger.info("finish to parse data ...");
        logger.info("correct data count:" + exactDataIndex + ", error data count:" + errorDataCount);

        /* check whether there is still data to insert */
        if (bulkinsertCount > 0) {
            if (bulkinsertCount < bulkinsertSize) {
                for (int i = 0; i < dataArr.length; i++) {
                    dataArr[i] = Arrays.copyOfRange(dataArr[i], 0, bulkinsertCount);
                }
            }
            OracleUtil.bulkinsert(connection, tableName, columns, dataArr);
        }

        OracleUtil.dropBulkInsertProcedure(connection, tableName, columns);
        return;
    }

    public static void onebyoneInsertImp(Connection connection, String tableName, List<String> columns, Map<String, Column> columnMap, DataHolder dataHolder) throws Exception {
        logger.info("start to build insert sqls ...");
        String insertSqlPrefix = ImportUtil.buildInsertSqlPrefix(tableName, columns, columnMap);

        List<String> sqls = new ArrayList<String>();
        for (int recordIndex = 0; recordIndex < dataHolder.getSize(); recordIndex++) {
            RowHolder row = dataHolder.getRow(recordIndex);
            StringBuffer sqlBuffer = new StringBuffer();
            sqlBuffer.append(insertSqlPrefix);

            for (int j = 0; j < columns.size(); j++) {
                String cname = columns.get(j);
                String cvalue = (String) row.get(j);
                Column column = columnMap.get(cname);
                if (j > 0) {
                    sqlBuffer.append(",");
                }
                ImportUtil.addColumnValue(sqlBuffer, recordIndex, cname, cvalue, column);
            }
            sqlBuffer.append(")");
            String sql = sqlBuffer.toString();

            sqls.add(sql);
        }
        logger.info("finish to build insert sqls ...");

        ImportUtil.executeSqls(connection, sqls);
    }

    public static boolean validateData(Connection connection, String userName, String tableName, List<String> columns, Map<String, Column> columnMap, DataHolder dataHolder) throws Exception {
        Set<String> primaryKeys = ImportUtil.getPrimaryKeys(connection, userName, tableName);
        Set<String> primaryKeyValueSet = new HashSet<String>();

        logger.info("start to validate data ...");
        int exactDataIndex = 0;
        int errorDataCount = 0;
        for (int recordIndex = 0; recordIndex < dataHolder.getSize(); recordIndex++) {
            RowHolder row = dataHolder.getRow(recordIndex);
            String primaryKeyValue = "";
            for (int i = 0; i < columns.size(); i++) {
                String cname = columns.get(i);
                String cvalue = (String) row.get(i);
                Column column = columnMap.get(cname);
                try {
                    Object value = ImportUtil.getSQLFormatedValue(recordIndex, cname, cvalue, column, true);
                    if (primaryKeys.contains(cname)) {
                        primaryKeyValue += value.toString();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage());
                    errorDataCount++;
                    continue;
                }
            }

            primaryKeyValue = primaryKeyValue.trim();
            if (primaryKeyValueSet.contains(primaryKeyValue)) {
                String errorMessage = "Row[" + recordIndex + "] duplicated primay key value:" + primaryKeyValue;
                logger.error(errorMessage);
                errorDataCount++;
            } else {
                primaryKeyValueSet.add(primaryKeyValue);
                exactDataIndex++; // increase when correct
            }
        }
        logger.info("finish to validate data ...");
        logger.info("correct data count:" + exactDataIndex + ", error data count:" + errorDataCount);

        return errorDataCount == 0;
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

    /**
     * remove special characters in column names and check duplicated columns
     */
    public static List<String> formatColumnNames(List<String> columns) throws Exception {

        Set<String> columnNameSet = new HashSet<String>();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            String col = column.replaceAll("\\W", "");
            if (columnNameSet.contains(col)) {
                throw new Exception("Exists duplicated column:" + col);
            }
            columnNameSet.add(col);
            columns.set(i, col);
        }
        return columns;
    }
}
