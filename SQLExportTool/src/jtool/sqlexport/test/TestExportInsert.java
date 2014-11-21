/**
 * Created Date: Dec 14, 2011 9:56:06 AM
 */
package jtool.sqlexport.test;

import jtool.sqlexport.ExportInsert;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class TestExportInsert {

	public static void main(String[] args) throws Exception {
		String[] params = new String[] { "com.microsoft.sqlserver.jdbc.SQLServerDriver",
				"jdbc:sqlserver://192.168.5.111:1433;DatabaseName=twse;SelectMethod=cursor", "twse", "password",
				"tb110", "select * from tb110" };
		ExportInsert.main(params);
	}
}
