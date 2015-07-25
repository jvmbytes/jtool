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

import jtool.sqlimport.domain.DataHolder;
import jtool.sqlimport.domain.RowHolder;

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
        final List<CSVRecord> records = parser.getRecords();
        DataHolder dataHolder = new DataHolder() {

            @Override
            public RowHolder getRow(int i) {
                final CSVRecord csvRecord = records.get(i);
                return new RowHolder() {

                    @Override
                    public int size() {
                        return csvRecord.size();
                    }

                    @Override
                    public Object get(int i) {
                        return csvRecord.get(i);
                    }
                };
            }

            @Override
            public int getSize() {
                return records.size();
            }
        };

        if (bulkinsert) {
            ImportUtil.bulkinsetImp(connection, tableName, columns, columnMap, dataHolder);
        } else {
            ImportUtil.onebyoneInsertImp(connection, tableName, columns, columnMap, dataHolder);
        }
    }

}
