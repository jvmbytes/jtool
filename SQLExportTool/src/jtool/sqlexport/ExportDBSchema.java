/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sqlexport;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

public class ExportDBSchema {

	/** 主键前缀 */
	private static final String TABLE_PRIMARY_KEY_PREFIX = "PK%";

	public static void main(String[] args) throws Exception {
		// 查询所有table的语句
		String sql_table = "select name from sysobjects where type = 'u' order by name";
		// 查询table所有字段和类型
		String sql_table_column =	" SELECT syscolumns.NAME "+
									"	,CASE  "+
									"		WHEN types.NAME='numeric' THEN "+
									"			types.NAME+'('+ CONVERT(varchar,syscolumns.prec)+',' + CONVERT(varchar,syscolumns.scale) +')' "+
									"		WHEN types.NAME='varchar' THEN "+
									"			types.NAME+'('+ CONVERT(varchar,syscolumns.length) +')' "+
									"		ELSE types.NAME END "+
									"	AS type "+
									" FROM syscolumns "+
									" LEFT JOIN sys.types ON sys.syscolumns.xtype = sys.types.system_type_id "+
									" WHERE syscolumns.id = (SELECT id FROM sysobjects WHERE NAME = ?)"; // 表名
		// 查询主键sql语句
		String sql_pk_key = "SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE i WHERE i.TABLE_NAME = ? AND i.CONSTRAINT_NAME like ?"; // 表名和约束前缀

		// 查询外键sql语句
		String sql_fk_key = "	SELECT ori_table,ori_column,ref_table,ref_column " +
							"	FROM ( " +
							"		SELECT  " +
							"			object_name(f.parent_object_id) AS ori_table " +
							"			,convert(SYSNAME, col_name(k.parent_object_id, k.parent_column_id)) AS ori_column " +
							"			,object_name(f.referenced_object_id) AS ref_table " +
							"			,convert(SYSNAME, col_name(k.referenced_object_id, k.referenced_column_id)) AS ref_column " +
							"			,object_name(f.object_id) AS fk_name " +
							"			,k.constraint_column_id AS fk_list_number " +
							"		FROM sys.foreign_keys f " +
							"		INNER JOIN sys.foreign_key_columns k ON f.object_id = k.constraint_object_id " +
							"			AND f.referenced_object_id = k.referenced_object_id " +
							"		) t " +
							"	WHERE ori_table= ?";

		String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		String linkUrl = "jdbc:sqlserver://localhost:1433;DatabaseName=mydatabase;SelectMethod=cursor";
		String userName = "sa";
		String password = "password";
		String saveFileName = "mydatabase";
		String outputFilePath = "./src/target/" + saveFileName + ".txt";

		Class.forName(driverName).newInstance();
		Connection myConn = DriverManager.getConnection(linkUrl, userName, password);
		PreparedStatement myStmt = null;
		PreparedStatement myStmt2 = null;
		ResultSet rs = null;
		ResultSet rs2 = null;

		// 查询所有表名
		System.out.println("start to add table name ... ");
		myStmt = myConn.prepareStatement(sql_table);
		rs = myStmt.executeQuery();
		int count_table = 0;
		List<String> tables = new ArrayList<String>();
		while (rs.next()) {
			count_table++;
			String temp_table_name = rs.getString(1);
			if (StringUtils.isNotBlank(temp_table_name)) {
				tables.add(temp_table_name.trim());
			}
		}
		System.out.println("Table count: " + count_table);
		System.out.println("end to add table name ... ");

		// 查询所有字段和字段类型
		System.out.println("start to add column name ... ");
		StringBuffer buffer_table = new StringBuffer();
		StringBuffer buffer_header = new StringBuffer();
		StringBuffer buffer_html = new StringBuffer();
		String table_name_tr = "<tr><td style='width:220px;'><b>表名</b></td><td colspan='4'>%s</td></tr>";
		buffer_header.append("<tr><td><b>備註</b></td><td colspan='4'>&nbsp;</td></tr>");
		buffer_header.append("<tr><td><b>實體</b></td><td colspan='4'>&nbsp;</td></tr>");
		buffer_header.append("<tr>");
		buffer_header.append("	<td style='width:220px;'><b>欄位名稱</b></td>");
		buffer_header.append("	<td style='width:150px;'><b>欄位說明</b></td>");
		buffer_header.append("	<td style='width:110px;'><b>類型</b></td>");
		buffer_header.append("	<td style='width:20px;'><b>NULL?</b></td>");
		buffer_header.append("	<td><b>備註</td>");
		buffer_header.append("</tr>");

		for (int i = 0; i < tables.size(); i++) {
			String table_name = tables.get(i);
			buffer_table.setLength(0);
			buffer_table.append("<h3 style='mso-list:level1;'>" + (i + 1) + ".&nbsp;" + table_name+"</h3><br />");
			buffer_table.append("<table border='1'>");
			buffer_table.append(String.format(table_name_tr, table_name));
			buffer_table.append(buffer_header);

			// 獲取主鍵
			List<String> pks = new ArrayList<String>();
			myStmt2 = myConn.prepareStatement(sql_pk_key);
			myStmt2.setString(1, table_name);
			myStmt2.setString(2, TABLE_PRIMARY_KEY_PREFIX);
			rs2 = myStmt2.executeQuery();
			while (rs2.next()) {
				String pk = rs2.getString(2);
				if (StringUtils.isNotBlank(pk)) {
					pks.add(pk.trim());
				}
			}

			// 獲取外鍵
			Map<String, String> fks = new HashMap<String, String>();
			myStmt2 = myConn.prepareStatement(sql_fk_key);
			myStmt2.setString(1, table_name);
			rs2 = myStmt2.executeQuery();
			while (rs2.next()) {
				String temp_ori_column_name = rs2.getString(2);
				String temp_ref_table_name = rs2.getString(3);
				String temp_ref_column_name = rs2.getString(4);
				if (StringUtils.isNotBlank(temp_ori_column_name) && StringUtils.isNotBlank(temp_ref_table_name) && StringUtils.isNotBlank(temp_ref_column_name)) {
					fks.put(temp_ori_column_name.trim(), temp_ref_table_name.trim() + ". " + temp_ref_column_name.trim());
				}
			}

			myStmt = myConn.prepareStatement(sql_table_column);
			myStmt.setString(1, table_name);
			rs = myStmt.executeQuery();
			while (rs.next()) {
				String temp_column_name = rs.getString(1);
				String temp_column_type = rs.getString(2);
				if (StringUtils.isNotBlank(temp_column_name) && StringUtils.isNotBlank(temp_column_type)) {
					StringBuffer buffer_td = new StringBuffer();
					String name = temp_column_name.trim();
					String type = temp_column_type.trim();

					buffer_td.append("<tr>");
					// 栏位名称
					buffer_td.append("	<td>");
					buffer_td.append(name);
					buffer_td.append("	</td>");
					// 栏位说明
					buffer_td.append("	<td>&nbsp;</td>");
					// 栏位类型
					buffer_td.append("	<td>");
					buffer_td.append(type);
					buffer_td.append("	</td>");
					// NULL?
					buffer_td.append("	<td>&nbsp;</td>");
					// 备注
					buffer_td.append("	<td>");

					StringBuffer buffer_key = new StringBuffer();
					if (pks != null && pks.size() > 0 && pks.contains(name)) {
						buffer_key.append("PK");
					}
					if (fks != null && fks.size() > 0 && fks.containsKey(name)) {
						String ref_str = fks.get(name);
						if (StringUtils.isNotBlank(ref_str)) {
							if (buffer_key.length() > 0) {
								buffer_key.append(",");
							}
							buffer_key.append("FK(" + ref_str + ")");
						}
					}
					if (buffer_key.length() == 0) {
						buffer_td.append("&nbsp;");
					} else {
						buffer_td.append(buffer_key);
					}
					buffer_td.append("	</td>");
					buffer_td.append("</tr>");
					buffer_table.append(buffer_td);
				}
			}
			buffer_table.append("</table>");
			buffer_html.append(buffer_table);
		}
		System.out.println("end to add column name ... ");

		System.out.println("start to write into file ... ");
		File file = new File(outputFilePath);
		System.out.println("file path:" + file.getAbsolutePath());
		FileUtils.writeStringToFile(file, buffer_html.toString(), "UTF-8");
		System.out.println("end to write into file ... ");
		rs.close();
		rs2.close();
		myStmt.close();
		myStmt2.close();
		myConn.close();
		System.out.println("------------------------");
		System.out.println("over");
	}

	private static void printForeignKeys(Connection connection, String tableName) throws SQLException {
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet foreignKeys = metaData.getImportedKeys(connection.getCatalog(), null, tableName);
		while (foreignKeys.next()) {
			String fkTableName = foreignKeys.getString("FKTABLE_NAME");
			String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
			String pkTableName = foreignKeys.getString("PKTABLE_NAME");
			String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");
			System.out.println(fkTableName + "." + fkColumnName + " -> " + pkTableName + "." + pkColumnName);
		}
	}
	
	// 拷贝到word，格式化需按照以下格式输出
	// <h3 style="mso-list:level1;">1.aaaa</h3>
	// <table border='1'>
	// <tr><td>asdf</td><td>ad</td></tr>
	// <tr><td>asdf</td><td>ad</td></tr>
	// </table>
}
