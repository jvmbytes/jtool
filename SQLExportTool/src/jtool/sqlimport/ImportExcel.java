/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sqlimport;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import jtool.excel.ExcelUtil;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It's just for oracle right now!
 * 
 * @author Geln Yang
 * @version 1.0
 */
public class ImportExcel {

    private static final Logger logger = LoggerFactory.getLogger(ImportExcel.class);

    public static void main(String[] args) throws Exception {
        String driverName = args[0];
        String linkUrl = args[1];
        String userName = args[2];
        String password = args[3];
        String tableName = args[4];
        String excelFilePath = args[5];
        String fileEncode = args[6];
        if (StringUtils.isBlank(fileEncode)) {
            fileEncode = "UTF-8";
        }

        logger.info("----------------------");
        logger.info(driverName);
        logger.info(linkUrl);
        logger.info(userName);
        logger.info(password);
        logger.info(tableName);
        logger.info(excelFilePath);

        imp(driverName, linkUrl, userName, password, excelFilePath, tableName);

        logger.info("------------------------");
        logger.info("over");
    }

    public static void imp(String driverName, String linkUrl, String userName, String password, String tableName, String excelFilePath) throws Exception {
        Class.forName(driverName).newInstance();
        Connection connection = DriverManager.getConnection(linkUrl, userName, password);

        File file = new File(excelFilePath);
        int numberOfSheets = ExcelUtil.getNumberOfSheets(file);
        for (int sheetIndex = 0; sheetIndex < numberOfSheets; sheetIndex++) {
            imp(connection, userName, tableName, file, sheetIndex);
        }

        connection.close();
    }

    @SuppressWarnings({"rawtypes"})
    public static void imp(Connection connection, String userName, String tableName, File file, int sheetIndex) throws Exception {
        Map<String, Column> columnTypeMap = ImportUtil.getColumnMap(connection, userName, tableName);

        logger.info("start import sheet:" + sheetIndex);
        List<List<Object>> lines = ExcelUtil.readExcelLines(file, sheetIndex, 0);
        if (lines == null || lines.size() < 2) {
            logger.info("no content in sheet:" + sheetIndex);
            return;
        }
        List columns = lines.get(0);

        List<List<Object>> dataList = lines.subList(1, lines.size());

        String insertSqlPrefix = ImportUtil.buildInsertSqlPrefix(tableName, columns, columnTypeMap);

        List<String> sqls = new ArrayList<String>();
        for (int recordIndex = 0; recordIndex < dataList.size(); recordIndex++) {
            List<Object> line = dataList.get(recordIndex);
            String cname = columns.get(0).toString();
            String cvalue = line.get(0).toString();
            Column column = columnTypeMap.get(cname);

            StringBuffer sqlBuffer = new StringBuffer();
            sqlBuffer.append(insertSqlPrefix);
            ImportUtil.addColumnValue(sqlBuffer, recordIndex, cname, cvalue, column); // add
                                                                                      // first

            for (int j = 1; j < columns.size(); j++) {
                cname = columns.get(j).toString();
                cvalue = line.get(j).toString();
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
