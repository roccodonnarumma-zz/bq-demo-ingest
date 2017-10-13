package com.google.demo.bq.ingest.model;

import java.nio.file.Path;
import java.util.List;

public class Table {

    private String prefix;
    private String name;
    private String bigqueryName;
    private List<Column> columns;
    private Path path;

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

    public String getBigqueryName() {
        return bigqueryName;
    }

    public void setBigqueryName(String bigqueryName) {
        this.bigqueryName = bigqueryName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }
}
