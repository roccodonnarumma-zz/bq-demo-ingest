package com.google.demo.bq.ingest.service;

import com.google.cloud.bigquery.*;
import com.google.demo.bq.ingest.model.Table;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class BigQueryService implements Callable<Boolean> {

    private Logger logger = LogManager.getLogger();
    private BigQuery bigQuery;

    private Table table;

    public BigQueryService(Table table) {
        this.table = table;
        BigQueryOptions options = BigQueryOptions.newBuilder().setProjectId("bi-poc-174212").build();
        bigQuery = new BigQueryOptions.DefaultBigqueryFactory().create(options);
    }

    @Override
    public Boolean call() throws Exception {
        String connectionString =
                "jdbc:sqlserver://LADBROKES-POC:1433;"
                        + "encrypt=true;trustServerCertificate=true;"
                        + "database=OmniCustomer;"
                        + "integratedSecurity=true;";

        // Declare the JDBC objects.
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            logger.log(Level.INFO, "Exporting table " + table.getPrefix() + "." + table.getName().replaceAll("\\)", ""));
            TableId tableId = TableId.of(table.getPrefix(), table.getName().replaceAll("\\)", ""));
            com.google.cloud.bigquery.Table bqTable = bigQuery.getTable(tableId);
            Schema schema = bqTable.getDefinition().getSchema();
            List<Field> fields = schema.getFields();

            statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT * FROM [OmniCustomer].[" + table.getPrefix() + "].[" + table.getName() + "]");

            List<Map<String, Object>> rowsContent = new ArrayList<>();

            while (resultSet.next()) {
                Map<String, Object> rowContent = new HashMap<>();
                for (int i = 1; i <= fields.size(); i++) {
                    if (resultSet.getString(i) != null && !resultSet.getString(i).isEmpty()) {
                        rowContent.put(fields.get(i - 1).getName(), resultSet.getString(i));
                    }
                }
                rowsContent.add(rowContent);

                if (rowsContent.size() == 5000) {
                    pushToBigquery(rowsContent, tableId);
                    rowsContent = new ArrayList<>();
                }
            }

            pushToBigquery(rowsContent, tableId);
        } catch (Exception e) {
            e.printStackTrace();
            return Boolean.FALSE;
        } finally {
            if (connection != null) try {
                connection.close();
            } catch (Exception e) {}
        }

        return Boolean.TRUE;
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
}
