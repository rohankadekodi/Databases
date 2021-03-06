package com.postgres.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.postgres.db.JdbcDBConstants;

public final class JdbcDBCreateTable {

	private static void usageMessage() {
		System.out.println("Create Table Client. Options:");
		System.out.println("  none");
		System.out.println("  columnA");
		System.out.println("  columnB");
		System.out.println("  both");
	}

	private static void createTable(String tablename, int index) throws SQLException {
		String url = JdbcDBConstants.CONNECTION_URL;
    
		if (url == null) {
			throw new SQLException("Missing connection information.");
		}

		Connection conn = null;

		conn = DriverManager.getConnection(url, JdbcDBConstants.CONNECTION_USER, JdbcDBConstants.CONNECTION_PASSWD);
		Statement stmt = conn.createStatement();
    
		StringBuilder sql = new StringBuilder("DROP TABLE IF EXISTS ");
		sql.append(tablename);
		sql.append(";");

		stmt.execute(sql.toString());

		sql = new StringBuilder("CREATE TABLE ");
		sql.append(JdbcDBConstants.TABLE_NAME);
		sql.append(" (theKey INTEGER PRIMARY KEY");
		sql.append(", columnA INTEGER");
		sql.append(", columnB INTEGER");
		sql.append(", filler CHAR(247)");
    
		sql.append(");");

		stmt.execute(sql.toString());

		System.out.println("Table " + tablename + " created..");
		sql.setLength(0);

		if (index == 1 || index == 3) {
			sql.append("CREATE INDEX columnA_index ON ");
			sql.append(JdbcDBConstants.TABLE_NAME);
			sql.append(" (columnA);");
			stmt.execute(sql.toString());
			sql.setLength(0);
			System.out.println("Index columnA_index created..");
		}

		if (index == 2 || index == 3) {
			sql.append("CREATE INDEX columnB_index ON ");
			sql.append(JdbcDBConstants.TABLE_NAME);
			sql.append(" (columnB);");
			stmt.execute(sql.toString());
			sql.setLength(0);
			System.out.println("Index columnB_index created..");
		}
		
		if (conn != null) {
			System.out.println("Closing database connection.");
			conn.close();
		}
	}

	public static void main(String[] args) {

		int index = 0;
		
		if (args.length == 0) {
			usageMessage();
			System.exit(0);
		}

		String tablename = JdbcDBConstants.TABLE_NAME;

		// parse arguments
		if (args[0].compareTo("none") == 0)
			index = 0;
		else if (args[0].compareTo("columnA") == 0)
			index = 1;
		else if (args[0].compareTo("columnB") == 0)
			index = 2;
		else
			index = 3;
		
		try {
			createTable(tablename, index);
		} catch (SQLException e) {
			System.err.println("Error in creating table. " + e);
			System.exit(1);
		}
	}

	private JdbcDBCreateTable() {
		super();
	}
}
