package com.postgres.db;

import com.postgres.db.JdbcDBConstants;
import com.postgres.db.DBOperations;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class JdbcDBClient {

	private static final String DEFAULT_PROP = "";
	private static DBOperations dbOps;

	private static String insertSQL;
	private static String readFirstSQL;
	private static String readSecondSQL;
	private static String readThirdSQL;
	private static String truncateSQL;
	
	private static Connection conn;
	private static long numRowsInBatch = 0;

	private static PreparedStatement cachedStatement = null;
	
	public static void init() {

		String url = JdbcDBConstants.CONNECTION_URL;
		dbOps = new DBOperations();
		
		try {
			conn = DriverManager.getConnection(url, JdbcDBConstants.CONNECTION_USER, JdbcDBConstants.CONNECTION_PASSWD);
			insertSQL = dbOps.createInsertStatement();
			readFirstSQL = dbOps.createFirstSelectQuery();
			readSecondSQL = dbOps.createSecondSelectQuery();
			readThirdSQL = dbOps.createThirdSelectQuery();
			truncateSQL = dbOps.createTruncateStatement();
			
		} catch (SQLException e) {
			System.err.println("Error in database operation: " + e);
			System.exit(-1);
		}
	}

	public static int read(int value, int queryNum) {

		PreparedStatement readStatement;
		try {
			if (queryNum == 1)
				readStatement = conn.prepareStatement(readFirstSQL);
			else if (queryNum == 2)
				readStatement = conn.prepareStatement(readSecondSQL);
			else
				readStatement = conn.prepareStatement(readThirdSQL);

			readStatement.setInt(1, value);
			if (queryNum == 3)
				readStatement.setInt(2, value);
			
			ResultSet resultSet = readStatement.executeQuery();
		        if (!resultSet.next()) {
				resultSet.close();
				return 0;
			}
			resultSet.close();
			return 0;
		} catch (SQLException e) {
			System.err.println("Error in processing read of table " + JdbcDBConstants.TABLE_NAME + ": " + e);
			return -1;
		}
	}

	public static int insert(int key, int columnA, int columnB, String filler) {

		PreparedStatement insertStatement;		
		try {			
			if (cachedStatement != null)
				insertStatement = cachedStatement;
			else {
				insertStatement = conn.prepareStatement(insertSQL);
				cachedStatement = insertStatement;
			}
			
			insertStatement.setInt(1, key);
			insertStatement.setInt(2, columnA);
			insertStatement.setInt(3, columnB);
			insertStatement.setString(4, filler);
			insertStatement.addBatch();

			if (++numRowsInBatch == JdbcDBConstants.BATCH_SIZE) {
				int[] results = insertStatement.executeBatch();
				for (int r : results) {
					if (r != 1 && r != 2)
						return -1;
				}
				numRowsInBatch = 0;
				cachedStatement = null;
			}			
			return 0;			
		} catch (SQLException e) {
			System.err.println("Error in processing insert to table: " + JdbcDBConstants.TABLE_NAME + e);
			return -1;
		}
	}

	public static int truncate() {

		PreparedStatement truncateStatement;		
		try {			
			truncateStatement = conn.prepareStatement(truncateSQL);		
			int result = truncateStatement.executeUpdate();
			return 0;			
		} catch (SQLException e) {
			System.err.println("Error in processing truncate to table: " + JdbcDBConstants.TABLE_NAME + e);
			return -1;
		}
	}

	
	public static void main(String[] args) {

		int i = 0, columnA = 0, columnB = 0, numRuns = 10, value = 0, argIndex = 0, queryNum = 0;
		Random rand = new Random();
		StringBuilder fillerSB = new StringBuilder();
		String filler = "";

		List<Integer> keys = new ArrayList<Integer>();
		List<Integer> queryOneValues = new ArrayList<Integer>() {{ add(5000); add(15000); add(25000); add(35000); add(45000); add(7500); add(12500); add(17500); add(22500); add(27500); }};
		List<Integer> queryTwoValues = new ArrayList<Integer>() {{ add(7000); add(14000); add(21000); add(28000); add(42000); add(49000); add(9000); add(16000); add(23000); add(30000); }};
		List<Integer> queryThreeValues = new ArrayList<Integer>() {{ add(9000); add(18000); add(27000); add(36000); add(46000); add(11000); add(20000); add(34000); add(38000); add(50000); }};
				
		if (args.length != 2) {
			System.out.println("Usage: JdbcDBClient.java <seq/rand> <1/2/3>");
			System.exit(-1);
		}
		
		init();

		queryNum = Integer.parseInt(args[1]);
		
		for (i = 0; i < 247; i++) {
			fillerSB.append("A");
		}

		for (i = 0; i < JdbcDBConstants.NUM_KEYS; i++) {
			keys.add(i+1);
		}

		filler = fillerSB.toString();

		// If random load specified, shuffle the keys
		if (args[0].compareTo("rand") == 0)
			Collections.shuffle(keys);

		System.out.println("Executing program with Select Query Number = " + queryNum + " and physical layout = " + args[0]);
		
		// Load the database
		long startTimeInsert = Calendar.getInstance().getTime().getTime();
		for (i = 0; i < JdbcDBConstants.NUM_KEYS; i++) {
			columnA = rand.nextInt(50000) + 1;
			columnB = rand.nextInt(50000) + 1;
			if (insert(keys.get(i), columnA, columnB, filler) != 0) {
				System.out.println("Insertion failed for key " + keys.get(i));
				System.exit(-1);
			}
		}
		long endTimeInsert = Calendar.getInstance().getTime().getTime();
		long timeElapsedInsert = endTimeInsert - startTimeInsert;
		System.out.println("Load DB executed in " + timeElapsedInsert + "ms");

		/*
		// Run Query
		for (i = 0; i < numRuns; i++) {
			long startTimeSelect = Calendar.getInstance().getTime().getTime();
			if (queryNum == 1)
				value = queryOneValues.get(i);
			else if (queryNum == 2)
				value = queryTwoValues.get(i);
			else
				value = queryThreeValues.get(i);

			if (read(value, queryNum) != 0) {
				System.out.println("Read failed for value = " + value + ", queryNum = " + queryNum);
				System.exit(-1);
			}
			long endTimeSelect = Calendar.getInstance().getTime().getTime();
			long timeElapsedSelect = endTimeSelect - startTimeSelect;
			System.out.println("Select Query executed in " + timeElapsedSelect + "ms");
		}

		if (truncate() != 0) {
			System.out.println("Truncation of table failed.");
			System.exit(-1);
		}
		*/
	}
}
