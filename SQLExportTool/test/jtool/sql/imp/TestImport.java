/**
 * Created By: Comwave Project Team Created Date: 2015年6月23日
 */
package jtool.sql.imp;

import jtool.sql.imp.ImportCSV;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class TestImport {

    public static void main(String[] args) throws Exception {
        String[] arguments = new String[] {"oracle.jdbc.driver.OracleDriver" //
                , "jdbc:oracle:thin:@db:1521:ORCL"//
                , "test"//
                , "password"//
                , "test"//
                , "C:\\Temp\\test.csv"//
                , "UTF-8"};
        ImportCSV.main(arguments);
    }
}
