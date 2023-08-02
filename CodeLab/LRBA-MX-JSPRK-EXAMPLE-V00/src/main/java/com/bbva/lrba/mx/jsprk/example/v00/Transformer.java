package com.bbva.lrba.mx.jsprk.example.v00;

import com.bbva.lrba.spark.transformers.Transform;
import com.bbva.lrba.mx.jsprk.example.v00.model.RowData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> dataset1 = datasetsFromRead.get("sourceAlias1");
        Dataset<Row> dataset2 = datasetsFromRead.get("sourceAlias2");
        Dataset<Row> dataset3 = datasetsFromRead.get("sourceAlias3");

        Dataset<Row> joinDNI = dataset1.join(dataset2,"DNI");
        Dataset<Row> joinDNI3 = dataset1.join(dataset3,"DNI");

        joinDNI3.show();


        return datasetsToWrite;
    }



}