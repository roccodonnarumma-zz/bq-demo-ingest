package com.google.demo.bq.ingest.model;

import java.util.List;

public class Table {

    private String prefix;
    private String name;
    private List<Column> columns;

    public Table(String prefix, String name) {
        this.prefix = prefix;
        this.name = name;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
