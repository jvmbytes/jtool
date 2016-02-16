/**
 * Created By: Comwave Project Team Created Date: 2015年7月25日
 */
package jtool.sqlimport;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import jtool.sql.domain.Column;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class OracleInsertUtil {
    private static final Logger logger = LoggerFactory.getLogger(OracleInsertUtil.class);

    public static String createBulkInsertProcedure(Connection connection, String tableName, List<String> columns, Map<String, Column> columnMap) throws Exception {
        Statement statement = connection.createStatement();
        String procedureName = getBulkInsertProcedureName(tableName);
        logger.info("start to create bulk insert procedure:" + procedureName);

        StringBuffer procedureBuffer = new StringBuffer();
        StringBuffer insertBuffer = new StringBuffer();
        StringBuffer valuesBuffer = new StringBuffer();
        procedureBuffer.append("create or replace procedure " + procedureName + "( ");
        insertBuffer.append("insert into " + tableName + "(");
        for (int i = 0; i < columns.size(); i++) {
            String col = columns.get(i);
            String typeName = getTypeName(tableName, i);
            Column column = columnMap.get(col);
            if (column == null) {
                throw new Exception("can't find column[" + col + "]");
            }
            String type = column.getType();
            int size = column.getSize();
            int decimalDigits = column.getDecimalDigits();

            String columnTypeDesc;
            if (type.contains("TIMESTAMP") || type.contains("DATE")) {
                columnTypeDesc = type;
            } else if (type.contains("NUMBER")) {
                columnTypeDesc = type + "(" + size + "," + decimalDigits + ")";
            } else if (type.contains("CHAR")) {
                columnTypeDesc = type + "(" + size + " CHAR)";
            } else {
                columnTypeDesc = type + "(" + size + ")";
            }

            String sql = "create or replace type " + typeName + " is table of " + columnTypeDesc;
            logger.debug(sql);
            statement.execute(sql);

            procedureBuffer.append("v" + i + " " + typeName);
            insertBuffer.append(col);
            valuesBuffer.append("v" + i + "(i)");
            if (i < columns.size() - 1) {
                procedureBuffer.append(",");
                insertBuffer.append(",");
                valuesBuffer.append(",");
            }

        }

        insertBuffer.append(") values(" + valuesBuffer + ");");

        procedureBuffer.append(") as ");
        // procedureBuffer.append("c integer;  ");
        procedureBuffer.append("begin  ");
        procedureBuffer.append("  forall i in 1.. v0.count  ");
        procedureBuffer.append(insertBuffer + "  ");
        procedureBuffer.append("end; ");

        statement.execute(procedureBuffer.toString());
        statement.close();
        logger.info("finish to create bulk insert procedure ...");
        return procedureName;
    }

    public static String dropBulkInsertProcedure(Connection connection, String tableName, List<String> columns) throws SQLException {
        Statement statement = connection.createStatement();
        String procedureName = getBulkInsertProcedureName(tableName);
        logger.info("start to drop bulk insert procedure:" + procedureName);
        statement.execute("drop procedure " + procedureName);
        for (int i = 0; i < columns.size(); i++) {
            String typeName = getTypeName(tableName, i);
            statement.execute("drop type " + typeName);
        }
        statement.close();
        logger.info("finish to drop bulk insert procedure ...");
        return procedureName;
    }

    /**
     * Oracle所有对象名称，字段名称有30个字符长度的限制
     */
    private static String getBulkInsertProcedureName(String tableName) {
        String name = "BI_" + tableName;
        if (name.length() > 30) {
            return name.substring(0, 30);
        }
        return name;
    }

    private static String getTypeName(String tableName, int i) {
        String name = tableName + "_T" + i;
        if (name.length() > 30) {
            return name.substring(name.length() - 30);
        }
        return name;
    }

    public static void bulkinsert(Connection connection, String tableName, List<String> columns, Object[][] dataArr) throws SQLException {
        int recordSize = dataArr[0].length;
        logger.info("start to bulk insert " + recordSize + " records ...");
        StringBuffer callBuffer = new StringBuffer();
        callBuffer.append("{ call " + getBulkInsertProcedureName(tableName) + "(");
        callBuffer.append("?");
        for (int i = 1; i < columns.size(); i++) {
            callBuffer.append(",?");
        }
        callBuffer.append(")}");
        CallableStatement cstmt = connection.prepareCall(callBuffer.toString());

        for (int i = 0; i < columns.size(); i++) {
            ArrayDescriptor type = oracle.sql.ArrayDescriptor.createDescriptor(getTypeName(tableName, i), connection);
            ARRAY arr = new ARRAY(type, connection, dataArr[i]);
            cstmt.setObject(i + 1, arr);
        }
        cstmt.execute();
        connection.commit();
        logger.info("finish to bulk insert " + recordSize + " records ...");
    }

    /**
     * ORA-00001: 违反唯一约束条件 (.) <br>
     * ORA-02261: 表中已存在这样的唯一关键字或主键<br>
     * ORA-02291:
     */
    public static boolean hasPkFkError(String message) {
        return message.contains("ORA-02261") || message.contains("ORA-00001") || message.contains("ORA-02291");
    }
}
