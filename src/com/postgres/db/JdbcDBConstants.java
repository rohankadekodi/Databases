package com.postgres.db;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class JdbcDBConstants {

	/** The URL to connect to the database. */
	public static final String CONNECTION_URL = "jdbc:postgresql://localhost:5432/hw_two";

	public static final String TABLE_NAME = "benchmark";
	
	/** The user name to use to connect to the database. */
	public static final String CONNECTION_USER = "postgres";

	/** The password to use for establishing the connection. */
	public static final String CONNECTION_PASSWD = "postgres";

	/** The batch size for batched inserts. Set to >0 to use batching */
	public static final String DB_BATCH_SIZE = "db.batchsize";

	/** The JDBC fetch size hinted to the driver. */
	public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

	/** The JDBC connection auto-commit property for the driver. */
	public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

	public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";

	/** The name of the property for the number of fields in a record. */
	public static final String FIELD_COUNT_PROPERTY = "4";

	/** Representing a NULL value. */
	public static final String NULL_VALUE = "NULL";

	/** The primary key in the user table. */
	public static final String PRIMARY_KEY = "theKey";

	public static final Integer NUM_KEYS = 5000000;

	public static final Integer BATCH_SIZE = 10000;
}
