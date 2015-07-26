/**
 * Created By: Comwave Project Team Created Date: 2015年7月25日
 */
package jtool.sqlimport;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class ImportScriptGenerator {
    static Pattern fileNamePattern = Pattern.compile("\\d+[\\-_]*([A-Z]+[A-Z_0-9]*)[^A-Z0-9_]*[^\\.]*\\.([A-Z]+)", Pattern.CASE_INSENSITIVE);

    public static void generateScript(String dataDir) throws Exception {
        File dir = new File(dataDir);
        String[] files = dir.list();
        for (String file : files) {
            Matcher matcher = fileNamePattern.matcher(file);
            if (!matcher.matches()) {
                throw new Exception("unknow file name format:" + file);
            }
            String tableName = matcher.group(1);
            String suffix = matcher.group(2);

            if (suffix.equalsIgnoreCase("csv")) {
                System.out.println("ImportCSV.imp(driver, url, user, password, \"" + tableName + "\", baseFilePath + \"" + file + "\", encode, true);");
            } else if (suffix.toLowerCase().contains("xls")) {
                System.out.println("ImportExcel.imp(driver, url, user, password, \"" + tableName + "\", baseFilePath + \"" + file + "\", true);");
            } else {
                throw new Exception("unknow file suffix:" + file);
            }
        }
    }
}
