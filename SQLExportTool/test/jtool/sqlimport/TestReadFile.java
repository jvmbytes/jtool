/**
 * Created By: Comwave Project Team Created Date: 2015年6月23日
 */
package jtool.sqlimport;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class TestReadFile {
    public static void main(String[] args) throws Exception {
        String cvsFilePath = "C:/Temp/test.csv";
        File file = new File(cvsFilePath);
        FileReader reader = new FileReader(file);
        CSVParser parser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());
        Map<String, Integer> headerMap = parser.getHeaderMap();
        System.out.println(headerMap);
        List<CSVRecord> records = parser.getRecords();
        System.out.println(records);
        parser.close();
    }
}
