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
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It's just for oracle right now!
 * 
 * @author Geln Yang
 * @version 1.0
 */
public class ImportCSV {
    private static final Logger logger = LoggerFactory.getLogger(ImportCSV.class);

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

        logger.info("----------------------");
        logger.info(driverName);
        logger.info(linkUrl);
        logger.info(userName);
        logger.info(password);
        logger.info(tableName);
        logger.info(cvsFilePath);

        imp(driverName, linkUrl, userName, password, tableName, cvsFilePath, fileEncode, true);

        logger.info("------------------------");
        logger.info("over");
    }

    @SuppressWarnings("resource")
    public static void imp(String driverName, String linkUrl, String userName, String password, String tableName, String cvsFilePath, String fileEncode, boolean bulkinsert) throws Exception {
        Class.forName(driverName).newInstance();
        Connection connection = DriverManager.getConnection(linkUrl, userName, password);

        Map<String, Column> columnMap = ImportUtil.getColumnMap(connection, userName, tableName);

        logger.info("start to parse file ...");
        File file = new File(cvsFilePath);
        Reader reader = new InputStreamReader(new FileInputStream(file), fileEncode);
        CSVParser parser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());
        Map<String, Integer> headerMap = parser.getHeaderMap();
        ArrayList<String> columns = new ArrayList<String>();
        columns.addAll(headerMap.keySet());
        logger.info("finish to parse file ...");
        List<CSVRecord> records = parser.getRecords();

        if (bulkinsert) {
            bulkinsetImp(connection, tableName, columns, columnMap, records);
        } else {
            onebyoneInsertImp(connection, tableName, columns, columnMap, records);
        }
    }

    private static void onebyoneInsertImp(Connection connection, String tableName, ArrayList<String> columns, Map<String, Column> columnMap, List<CSVRecord> records) throws Exception {
        logger.info("start to build insert sqls ...");
        String insertSqlPrefix = ImportUtil.buildInsertSqlPrefix(tableName, columns, columnMap);

        List<String> sqls = new ArrayList<String>();
        for (int recordIndex = 0; recordIndex < records.size(); recordIndex++) {
            CSVRecord csvRecord = records.get(recordIndex);
            String cname = columns.get(0);
            String cvalue = csvRecord.get(0);
            Column column = columnMap.get(cname);

            StringBuffer sqlBuffer = new StringBuffer();
            sqlBuffer.append(insertSqlPrefix);
            ImportUtil.addColumnValue(sqlBuffer, recordIndex, cname, cvalue, column); // add
                                                                                      // first

            for (int j = 1; j < columns.size(); j++) {
                cname = columns.get(j);
                cvalue = csvRecord.get(j);
                column = columnMap.get(cname);
                sqlBuffer.append(",");
                ImportUtil.addColumnValue(sqlBuffer, recordIndex, cname, cvalue, column);
            }
            sqlBuffer.append(")");
            String sql = sqlBuffer.toString();

            sqls.add(sql);
        }
        logger.info("finish to build insert sqls ...");

        ImportUtil.executeSqls(connection, sqls);
    }

    private static void bulkinsetImp(Connection connection, String tableName, ArrayList<String> columns, Map<String, Column> columnMap, List<CSVRecord> records) throws Exception {
        OracleUtil.createBulkInsertProcedure(connection, tableName, columns, columnMap);

        Object[][] dataArr = new Object[columns.size()][records.size()];
        for (int recordIndex = 0; recordIndex < records.size(); recordIndex++) {
            CSVRecord csvRecord = records.get(recordIndex);
            for (int i = 0; i < columns.size(); i++) {
                String cname = columns.get(i);
                String cvalue = csvRecord.get(i);
                Column column = columnMap.get(cname);

                Object value = ImportUtil.getSQLFormatedValue(recordIndex, cname, cvalue, column, true);
                dataArr[i][recordIndex] = value;
            }
        }

        OracleUtil.bulkinsert(connection, tableName, columns, dataArr);

        return;
    }

}
