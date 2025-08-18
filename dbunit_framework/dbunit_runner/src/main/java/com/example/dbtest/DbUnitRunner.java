package com.example.dbtest;

import org.dbunit.Assertion;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;

public class DbUnitRunner {
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: DbUnitRunner <jdbcUrl> <user> <pass> <flatXmlFile> <tableName> [excludeColumns]");
            System.exit(2);
        }
        final String jdbc = args[0];
        final String user = args[1];
        final String pass = args[2];
        final String flatXml = args[3];
        final String tableName = args[4];
        final List<String> excludes = args.length >= 6 && !args[5].isBlank()
                ? Arrays.asList(args[5].split(",")) : List.of();

        try (Connection conn = DriverManager.getConnection(jdbc, user, pass)) {
            DatabaseConnection dbConn = new DatabaseConnection(conn);
            DatabaseConfig config = dbConn.getConfig();
            config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());

            FlatXmlDataSetBuilder builder = new FlatXmlDataSetBuilder();
            builder.setColumnSensing(true);
            IDataSet expected = builder.build(new File(flatXml));
            ITable expectedTable = expected.getTable(tableName);

            IDataSet actualDs = dbConn.createDataSet(new String[]{tableName});
            ITable actualTable = actualDs.getTable(tableName);

            if (excludes.isEmpty()) {
                Assertion.assertEquals(expectedTable, actualTable);
            } else {
                Assertion.assertEqualsIgnoreCols(expectedTable, actualTable, excludes.toArray(new String[0]));
            }
            System.out.println("[DBUnit] OK: " + tableName);
        } catch (AssertionError ae) {
            System.err.println("[DBUnit] MISMATCH: " + tableName + " -> " + ae.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("[DBUnit] ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(3);
        }
    }
}
