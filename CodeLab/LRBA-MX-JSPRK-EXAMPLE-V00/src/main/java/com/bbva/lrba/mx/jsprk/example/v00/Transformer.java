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

        Dataset<Row> joinDNI = dataset1.join(dataset2,"DNI");
        Dataset<Row> filteredDS = joinDNI.select("DNI").where(joinDNI.col("DNI").equalTo("000001"));

        datasetsToWrite.put("targetAlias1",joinDNI);

        System.out.println("Transformer");
        System.out.println("Transformer count: " + joinDNI.count());
        joinDNI.show();

        System.out.println("Transformer columna Fecha");
        Dataset<Row> columnaFecha = joinDNI.withColumn("Fecha", functions.lit(functions.current_date()));
        columnaFecha.show();

        System.out.println("Transformer columna caso DNI");
        Dataset<Row> columnaDNI = columnaFecha.withColumn("Case",
                functions.when(functions.col("DNI").equalTo(1) ,200)
                        .otherwise(40))
                        .withColumn("DNI", functions.col("DNI").cast("Integer"));
        columnaDNI.show();

        System.out.println("Transformer agg function");
        columnaDNI.groupBy("Fecha").sum("DNI").drop("Fecha").show();
        columnaDNI.groupBy("Fecha").avg("DNI").drop("Fecha").show();
        columnaDNI.groupBy("Fecha").max("DNI").drop("Fecha").show();

        return datasetsToWrite;
    }



}