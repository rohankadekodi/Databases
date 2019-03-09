package com.postgres.db;
import com.postgres.db.JdbcDBConstants;

/**
 * A default flavor for relational databases.
 */
public final class DBOperations {
	public DBOperations() {
		super();
	}

	public String createInsertStatement() {
		StringBuilder insert = new StringBuilder("INSERT INTO ");
		insert.append(JdbcDBConstants.TABLE_NAME);
		insert.append(" (" + JdbcDBConstants.PRIMARY_KEY + "," + "columnA" + "," + "columnB" + "," + "filler" + ")");
		insert.append(" VALUES(?,?,?,?)");
		return insert.toString();
	}

	public String createFirstSelectQuery() {
		StringBuilder read = new StringBuilder("SELECT * FROM ");
		read.append(JdbcDBConstants.TABLE_NAME);
		read.append(" WHERE ");
		read.append(JdbcDBConstants.TABLE_NAME);
		read.append(".columnA");
		read.append(" = ");
		read.append("?");
		return read.toString();
	}

	public String createSecondSelectQuery() {
		StringBuilder read = new StringBuilder("SELECT * FROM ");
		read.append(JdbcDBConstants.TABLE_NAME);
		read.append(" WHERE ");
		read.append(JdbcDBConstants.TABLE_NAME);
		read.append(".columnB");
		read.append(" = ");
		read.append("?");
		return read.toString();
	}

	public String createThirdSelectQuery() {
		StringBuilder read = new StringBuilder("SELECT * FROM ");
		read.append(JdbcDBConstants.TABLE_NAME);
		read.append(" WHERE ");
		read.append(JdbcDBConstants.TABLE_NAME);
		read.append(".columnA");
		read.append(" = ");
		read.append("?");
		read.append(" AND ");
		read.append(JdbcDBConstants.TABLE_NAME);
		read.append(".columnB");
		read.append(" = ");
		read.append("?");
		return read.toString();
	}

	public String createTruncateStatement() {
		StringBuilder truncate = new StringBuilder("TRUNCATE TABLE ");
	        truncate.append(JdbcDBConstants.TABLE_NAME);
		return truncate.toString();
	}
}
