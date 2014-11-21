/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sqlimport;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class ImportCSV {

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
		String outputFilePath = "./" + tableName + ".csv";

		System.out.println(driverName);
		System.out.println(linkUrl);
		System.out.println(userName);
		System.out.println(password);
		System.out.println(tableName);
		System.out.println(cvsFilePath);
		System.out.println(outputFilePath);

		List<String> lines = FileUtils.readLines(new File(cvsFilePath), fileEncode);

		Class.forName(driverName).newInstance();
		Connection myConn = DriverManager.getConnection(linkUrl, userName, password);
		Statement myStmt = myConn.createStatement();

		String columns = lines.get(0);

		for (int i = 1; i < lines.size(); i++) {

		}

		myStmt.close();
		myConn.close();
		System.out.println("------------------------");
		System.out.println("over");
	}
}
