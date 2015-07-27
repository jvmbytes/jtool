/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sqlimport;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import jtool.excel.ExcelUtil;
import jtool.sqlimport.domain.DataHolder;
import jtool.sqlimport.domain.RowHolder;

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

        imp(driverName, linkUrl, userName, password, excelFilePath, tableName, true);

        logger.info("------------------------");
        logger.info("over");
    }

    public static void imp(String driverName, String linkUrl, String userName, String password, String tableName, String excelFilePath, boolean bulkinsert) throws Exception {
        Class.forName(driverName).newInstance();
        Connection connection = DriverManager.getConnection(linkUrl, userName, password);

        File file = new File(excelFilePath);
        int numberOfSheets = ExcelUtil.getNumberOfSheets(file);
        for (int sheetIndex = 0; sheetIndex < numberOfSheets; sheetIndex++) {
            imp(connection, userName, tableName, file, sheetIndex, bulkinsert);
        }

        connection.close();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void imp(Connection connection, String userName, String tableName, File file, int sheetIndex, boolean bulkinsert) throws Exception {
        Map<String, Column> columnMap = ImportUtil.getColumnMap(connection, userName, tableName);

        logger.info("start import sheet:" + sheetIndex);
        List<List<Object>> lines = ExcelUtil.readExcelLines(file, sheetIndex, 0);
        if (lines == null || lines.size() < 2) {
            logger.info("no content in sheet:" + sheetIndex);
            return;
        }

        List columns = lines.get(0);
        columns = ImportUtil.formatColumnNames(columns);

        final List<List<Object>> dataList = lines.subList(1, lines.size());

        DataHolder dataHolder = new DataHolder() {

            @Override
            public int getSize() {
                return dataList.size();
            }

            @Override
            public RowHolder getRow(int i) {
                final List<Object> list = dataList.get(i);
                return new RowHolder() {

                    @Override
                    public int size() {
                        return list.size();
                    }

                    @Override
                    public Object get(int i) {
                        if (list.size() <= i) {
                            return null;
                        }
                        return list.get(i);
                    }
                };
            }
        };

        if (ImportGlobals.isValidFirst()) {
            boolean valid = ImportUtil.validateData(connection, userName, tableName, columns, columnMap, dataHolder);
            if (!valid) {
                return; // STOP when not valid
            }
        }

        if (bulkinsert) {
            ImportUtil.bulkinsetImp(connection, userName, tableName, columns, columnMap, dataHolder);
        } else {
            ImportUtil.onebyoneInsertImp(connection, tableName, columns, columnMap, dataHolder);
        }
    }

}
