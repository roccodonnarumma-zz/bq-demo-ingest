package com.google.demo.bq.ingest;

import com.google.cloud.bigquery.*;
import com.google.demo.bq.ingest.model.Column;
import com.google.demo.bq.ingest.model.Table;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private Logger logger = LogManager.getLogger();
    private BigQuery bigQuery;
    private ExecutorService executorService;

    public static void main(String[] args) {
        Main main = new Main();

        main.start();
        //main.createSchema();
        main.generateCSVs();
//        main.loadData();
        main.cleanup();

        System.exit(0);
    }

    private void start() {
        BigQueryOptions options = BigQueryOptions.newBuilder().setProjectId("bi-poc-174212").build();
        bigQuery = new BigQueryOptions.DefaultBigqueryFactory().create(options);
        executorService = Executors.newFixedThreadPool(8);
    }

    private void createSchema() {
        try {
            logger.log(Level.INFO, "Creating schemas");

            String connectionString =
                    "jdbc:sqlserver://LADBROKES-POC:1433;"
                            + "encrypt=true;trustServerCertificate=true;"
                            + "database=OmniCustomer;"
                            + "integratedSecurity=true;";

            // Declare the JDBC objects.
            Connection connection = null;
            Statement statement = null;
            ResultSet resultSet = null;

            List<Table> tables = new ArrayList<>();

            try {
                // Establish the connection.
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                connection = DriverManager.getConnection(connectionString);

                DatabaseMetaData metaData = connection.getMetaData();
                String[] types = {"TABLE"};
                resultSet = metaData.getTables(null, null, "%", types);

                while(resultSet.next()) {
                    tables.add(new Table(resultSet.getString(2), resultSet.getString(3)));
                }

                for(Table table : tables) {
                    // Create and execute a SELECT SQL statement.
                    String selectSql = "SELECT TOP 50 * FROM [OmniCustomer].[" + table.getPrefix() + "].[" + table.getName() + "]";
                    statement = connection.createStatement();
                    resultSet = statement.executeQuery(selectSql);

                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    List<Column> columns = new ArrayList<>();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        columns.add(new Column(rsmd.getColumnName(i), rsmd.getColumnTypeName(i)));
                    }
                    table.setColumns(columns);
                }


                for(Table table : tables) {
                    DatasetInfo dataset = Dataset.newBuilder(table.getPrefix()).setLocation("EU").build();
                    try {
                        bigQuery.create(dataset);
                    } catch(Exception e) {}

                    TableId tableId = TableId.of(table.getPrefix(), table.getName().replaceAll("\\)", ""));
                    // Table field definition

                    List<Field> fields = new ArrayList<>();
                    for(Column column : table.getColumns()) {
                        fields.add(Field.of(column.getName().replaceAll(" ", ""), Field.Type.string()));
                    }
                    // Table schema definition
                    Schema schema = Schema.of(fields);
                    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
                    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
                    bigQuery.create(tableInfo);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                if (connection != null) try { connection.close(); } catch(Exception e) {}
            }
        } catch(Throwable throwable) {
            logger.log(Level.ERROR, throwable);
            throwable.printStackTrace();
        }
    }

    private void generateCSVs() {

    }

    private void loadData() {
        try {
            logger.log(Level.INFO, "Loading data");

            String connectionString =
                    "";

            // Declare the JDBC objects.
            Connection connection = null;
            Statement statement = null;
            ResultSet resultSet = null;

            List<Table> tables = new ArrayList<>();

            try {
                // Establish the connection.
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                connection = DriverManager.getConnection(connectionString);

                DatabaseMetaData metaData = connection.getMetaData();
                String[] types = {"TABLE"};
                resultSet = metaData.getTables(null, null, "%", types);

                while(resultSet.next()) {
                    tables.add(new Table(resultSet.getString(2), resultSet.getString(3)));
                }

                boolean load = false;
                for(Table table : tables) {
                    if("FactCustomerTransactions_Lads".equals(table.getName())) {
                        load = true;
                    }
                    if(load) {
                        logger.log(Level.INFO, "Exporting table " + table.getPrefix() + "." + table.getName().replaceAll("\\)", ""));
                        TableId tableId = TableId.of(table.getPrefix(), table.getName().replaceAll("\\)", ""));
                        com.google.cloud.bigquery.Table bqTable = bigQuery.getTable(tableId);
                        Schema schema = bqTable.getDefinition().getSchema();
                        List<Field> fields = schema.getFields();

                        statement = connection.createStatement();
                        resultSet = statement.executeQuery("SELECT * FROM [OmniCustomer].[" + table.getPrefix() + "].[" + table.getName() + "]");

                        List<Map<String, Object>> rowsContent = new ArrayList<>();

                        //                    executorService.invokeAll(callables)
                        //                            .stream()
                        //                            .map(future -> {
                        //                                try {
                        //                                    return future.get();
                        //                                } catch(Exception e) {
                        //                                    throw new IllegalStateException(e);
                        //                                }
                        //                            })
                        //                            .forEach(System.out::println);

                        while(resultSet.next()) {
                            Map<String, Object> rowContent = new HashMap<>();
                            for(int i = 1; i <= fields.size(); i++) {
                                if(resultSet.getString(i) != null && !resultSet.getString(i).isEmpty()) {
                                    rowContent.put(fields.get(i - 1).getName(), resultSet.getString(i));
                                }
                            }
                            rowsContent.add(rowContent);

                            if(rowsContent.size() == 5000) {
                                pushToBigquery(rowsContent, tableId);
                                rowsContent = new ArrayList<>();
                            }
                        }

                        pushToBigquery(rowsContent, tableId);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                if (connection != null) try { connection.close(); } catch(Exception e) {}
            }
        } catch(Throwable throwable) {
            logger.log(Level.ERROR, throwable);
            throwable.printStackTrace();
        }
    }

    private void pushToBigquery(List<Map<String, Object>> rowsContent, TableId tableId) {
        if(rowsContent.size() != 0) {
            InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
            for(Map<String, Object> content : rowsContent) {
                builder.addRow(content);
            }

            InsertAllResponse response = bigQuery.insertAll(builder.build());
            if (response.hasErrors()) {
                logger.log(Level.ERROR, "ROW-CONTENT");
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    logger.log(Level.ERROR, entry.getValue());
                }
            }
        }
    }



    private void cleanup() {
        logger.log(Level.INFO, "Finished import");
    }
}
