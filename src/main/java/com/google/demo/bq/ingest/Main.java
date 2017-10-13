package com.google.demo.bq.ingest;

import com.google.cloud.WaitForOption;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.*;
import com.google.demo.bq.ingest.model.Column;
import com.google.demo.bq.ingest.model.Table;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Main {

    private static final String PROJECT_ID = "";
    private static final String BUCKET_NAME = "";
    private static final String FOLDER_NAME = "";
    private static final String JDBC_HOSTNAME = "";
    private static final String JDBC_PORT = "";
    private static final String JDBC_DATABASE = "";

    private Logger logger = LogManager.getLogger();
    private BigQuery bigQuery;

    private static String connectionString;

    static {
        String.format("jdbc:sqlserver://%s:%s;"
                + "encrypt=true;trustServerCertificate=true;"
                + "database=%s;"
                + "integratedSecurity=true;", JDBC_HOSTNAME, JDBC_PORT, JDBC_DATABASE);
    }

    private Storage storage;

    public static void main(String[] args) {
        Main main = new Main();

        main.start();
        List<Table> tables = main.buildModel();
        main.createSchema(tables);
        main.generateCSVs(tables);
        main.uploadToGCS(tables);
        main.loadToBigQuery(tables);
        main.cleanup();

        System.exit(0);
    }

    private void start() {
        BigQueryOptions options = BigQueryOptions.newBuilder().setProjectId(PROJECT_ID).build();
        bigQuery = new BigQueryOptions.DefaultBigqueryFactory().create(options);
        storage = (Storage) StorageOptions.getDefaultInstance().getService();
    }

    private List<Table> buildModel() {
        logger.log(Level.INFO, "--------------------------------------------");
        logger.log(Level.INFO, "Building model");

        Connection connection = null;
        List<Table> tables = new ArrayList<>();

        try {
            URI uri = Main.class.getClassLoader().getResource("").toURI();
            String baseDirectory = Paths.get(uri).getParent().toString();

            // Establish the connection.
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connection = DriverManager.getConnection(connectionString);

            DatabaseMetaData metaData = connection.getMetaData();
            String[] types = {"TABLE"};
            ResultSet resultSet = metaData.getTables(null, null, "%", types);

            while (resultSet.next()) {
                tables.add(new Table(resultSet.getString(2), resultSet.getString(3)));
                //break;
            }

            for (Table table : tables) {
                // Create and execute a SELECT SQL statement.
                String selectSql = "SELECT TOP 50 * FROM [OmniCustomer].[" + table.getPrefix() + "].[" + table.getName() + "]";
                Statement statement = connection.createStatement();
                resultSet = statement.executeQuery(selectSql);

                ResultSetMetaData rsmd = resultSet.getMetaData();
                List<Column> columns = new ArrayList<>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    columns.add(new Column(rsmd.getColumnName(i), rsmd.getColumnTypeName(i)));
                }
                table.setColumns(columns);
                table.setBigqueryName(table.getName().replaceAll("\\)", ""));

                Path path = Paths.get(baseDirectory + File.separator + table.getPrefix() + "-" + table.getBigqueryName() + ".txt");
                table.setPath(path);
            }
        } catch(Throwable e) {
            logger.log(Level.ERROR, e);
            e.printStackTrace();
        } finally {
            if (connection != null) try { connection.close(); } catch(Exception e) {}
        }

        return tables;
    }

    private void createSchema(List<Table> tables) {
        try {
            logger.log(Level.INFO, "--------------------------------------------");
            logger.log(Level.INFO, "Creating schemas");

            Set<String> datasets = new HashSet<>();

            for(Table table : tables) {
                DatasetInfo dataset = Dataset.newBuilder(table.getPrefix()).setLocation("EU").build();

                if(!datasets.contains(dataset.getDatasetId().getDataset())) {
                    Dataset existingDataset = bigQuery.getDataset(dataset.getDatasetId());
                    if (existingDataset != null && existingDataset.exists()) {
                        logger.log(Level.INFO, String.format("SKIPPED - Dataset %s already exists", existingDataset.getDatasetId().getDataset()));
                    } else {
                        bigQuery.create(dataset);
                        logger.log(Level.INFO, String.format("Created Dataset %s", dataset.getDatasetId().getDataset()));
                    }

                    datasets.add(dataset.getDatasetId().getDataset());
                }

                TableId tableId = TableId.of(table.getPrefix(), table.getBigqueryName());

                com.google.cloud.bigquery.Table existingTable = bigQuery.getTable(tableId);
                if(existingTable != null && existingTable.exists()) {
                    logger.log(Level.INFO, String.format("SKIPPED - Table %s already exists", existingTable.getTableId().getTable()));
                } else {
                    // Table field definition
                    List<Field> fields = new ArrayList<>();
                    for (Column column : table.getColumns()) {
                        fields.add(Field.of(column.getName().replaceAll(" ", ""), Field.Type.string()));
                    }
                    // Table schema definition
                    Schema schema = Schema.of(fields);
                    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
                    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
                    bigQuery.create(tableInfo);
                    logger.log(Level.INFO, String.format("Created Table %s", tableInfo.getTableId().getTable()));
                }
            }
        } catch(Throwable throwable) {
            logger.log(Level.ERROR, throwable);
            throwable.printStackTrace();
        }
    }

    private void generateCSVs(List<Table> tables) {
        Connection connection = null;
        try {
            logger.log(Level.INFO, "--------------------------------------------");
            logger.log(Level.INFO, "Creating CSVs");

            for(Table table : tables) {
                if(table.getPath().toFile().exists()) {
                    logger.log(Level.INFO, String.format("SKIPPED - CSV %s already exists", table.getPath().getFileName().toString()));
                    continue;
                }

                logger.log(Level.INFO, "Generating CSV for table " + table.getPrefix() + "." + table.getBigqueryName());

                // Establish the connection.
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                connection = DriverManager.getConnection(connectionString);

                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM [OmniCustomer].[" + table.getPrefix() + "].[" + table.getName() + "]");

                try (BufferedWriter writer = Files.newBufferedWriter(table.getPath())) {
                    while(resultSet.next()) {
                        StringBuilder sb = new StringBuilder();
                        for(int i = 1; i <= table.getColumns().size(); i++) {
                            sb.append(resultSet.getString(i));
                            if(i != table.getColumns().size()) {
                                sb.append(",");
                            }
                        }

                        writer.write(sb.toString());
                        writer.newLine();
                    }
                }
            }
        } catch(Throwable throwable) {
            logger.log(Level.ERROR, throwable);
            throwable.printStackTrace();
        } finally {
            if (connection != null) try { connection.close(); } catch(Exception e) {}
        }
    }

    private void uploadToGCS(List<Table> tables) {
        logger.log(Level.INFO, "--------------------------------------------");
        logger.log(Level.INFO, "Uploading CSVs to GCS");

        for(Table table : tables) {
            String blobName = FOLDER_NAME + "/" + table.getPath().getFileName().toString();
            BlobId blobId = BlobId.of(BUCKET_NAME, blobName);

            Blob blob = storage.get(blobId);
            if(blob != null && blob.exists()) {
                logger.log(Level.INFO, String.format("SKIPPED - CSV %s in GCS already exists", table.getPath().getFileName().toString()));
                continue;
            }

            logger.log(Level.INFO, String.format("Uploading to GCS - %s", table.getPath().getFileName().toString()));

            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                   .setContentType("text/plain")
                   .build();

            try(WriteChannel writer = storage.writer(blobInfo)) {
                byte[] buffer = new byte[1024];
                try (InputStream input = Files.newInputStream(table.getPath())) {
                    int limit;
                    while ((limit = input.read(buffer)) >= 0) {
                        try {
                            writer.write(ByteBuffer.wrap(buffer, 0, limit));
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            } catch (IOException e) {
                logger.log(Level.ERROR, "Error reading file: " + table.getPath().toString());
            }
        }
    }

    private void loadToBigQuery(List<Table> tables) {
        logger.log(Level.INFO, "--------------------------------------------");
        logger.log(Level.INFO, "Submitting BigQuery load jobs");

        for (Table table : tables) {
            logger.log(Level.INFO, String.format("Loading to BigQuery %s", table.getPath().getFileName().toString()));
            try {
                if (table.getPath() != null) {
                    TableId tableId = TableId.of(table.getPrefix(), table.getBigqueryName());
                    LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId,
                            String.format("gs://%s/%s/%s",
                                    BUCKET_NAME,
                                    FOLDER_NAME,
                                    table.getPath().getFileName().toString()));

                    Job loadJob = bigQuery.create(JobInfo.newBuilder(loadConfig).build());
                    loadJob = loadJob.waitFor(WaitForOption.timeout(10, TimeUnit.SECONDS));
                }
            } catch(InterruptedException | TimeoutException e) {
                logger.log(Level.ERROR, "Timeout waiting for BQ load job for file: " + table.getPath().getFileName().toString());
            }
        }
    }

    private void cleanup() {
        logger.log(Level.INFO, "Finished import");
    }
}
