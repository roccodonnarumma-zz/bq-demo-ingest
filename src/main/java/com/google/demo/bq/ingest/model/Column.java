package com.google.demo.bq.ingest.model;

import com.google.cloud.bigquery.Field;

public class Column {

    private String name;
    private Field.Type type;

    public Column(String name, String type) {
        this.name = name;
        switch(type) {
            case "int":
                this.type = Field.Type.integer();
                break;
            case "smallint":
                this.type = Field.Type.integer();
                break;
            case "bigint":
                this.type = Field.Type.integer();
                break;
            case "numeric":
                this.type = Field.Type.integer();
                break;
            case "date":
                this.type = Field.Type.timestamp();
                break;
            case "datetime":
                this.type = Field.Type.timestamp();
                break;
            case "datetime2":
                this.type = Field.Type.timestamp();
                break;
            case "float":
                this.type = Field.Type.floatingPoint();
                break;
            case "decimal":
                this.type = Field.Type.floatingPoint();
                break;
            default:
                this.type = Field.Type.string();
                break;
        }
    }

    public String getName() {
        return name;
    }

    public Field.Type getType() {
        return type;
    }
}
