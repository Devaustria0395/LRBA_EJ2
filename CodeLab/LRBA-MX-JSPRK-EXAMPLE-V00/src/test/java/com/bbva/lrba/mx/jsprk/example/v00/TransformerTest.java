package com.bbva.lrba.mx.jsprk.example.v00;

import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
import com.bbva.lrba.mx.jsprk.example.v00.model.RowData;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.avg;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransformerTest extends LRBASparkTest {

    private Transformer transformer;

    @BeforeEach
    void setUp() {
        this.transformer = new Transformer();
    }

    @Test
    void transform_Output() {
        StructType schema = DataTypes.createStructType(
               new StructField[]{
                         DataTypes.createStructField("ENTIDAD", DataTypes.StringType, false),
                         DataTypes.createStructField("DNI", DataTypes.StringType, false),
                         DataTypes.createStructField("NOMBRE", DataTypes.StringType, false),
                         DataTypes.createStructField("TELEFONO", DataTypes.StringType, false)
               });
        Row firstRow = RowFactory.create("0182", "000001",  "John Doe", "123-456");
        Row secondRow = RowFactory.create("0182", "000002", "Mike Doe", "123-567");
        Row thridrow = RowFactory.create("0182", "000003", "Paul Doe", "123-678");

        StructType schema2 = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("DNI", DataTypes.StringType, false),
                        DataTypes.createStructField("EMAIL", DataTypes.StringType, false)
                });
        Row firstRow2 = RowFactory.create("000001", "johndoe@gmail.com");
        Row secondRow2 = RowFactory.create("000002", "mikedoe@gmail.com");
        Row thirdRow2 = RowFactory.create("000003", "pauldoe@gmail.com");

        StructType schema3 = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("DNI", DataTypes.StringType, false),
                        DataTypes.createStructField("VIP", DataTypes.StringType, false),
                        DataTypes.createStructField("EMAIL", DataTypes.StringType, false),
                        DataTypes.createStructField("PAYMETHOD", DataTypes.StringType, false)
                });
        Row firstRow3 = RowFactory.create("000001", "YES","johndoe@gmail.com","debit card");
        Row secondRow3 = RowFactory.create("000002", "NO","mikedoe@gmail.com","credit card");
        Row thirdRow3 = RowFactory.create("000003", "YES","pauldoe@gmail.com","paypal");

        final List<Row> listRows = Arrays.asList(firstRow,secondRow,thridrow);
        final List<Row> listRows2 = Arrays.asList(firstRow2,secondRow2,thirdRow2);
        final List<Row> listRows3 = Arrays.asList(firstRow3,secondRow3,thirdRow3);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> dataset = datasetUtils.createDataFrame(listRows, schema);
        Dataset<Row> dataset2 = datasetUtils.createDataFrame(listRows2, schema2);
        Dataset<Row> dataset3 = datasetUtils.createDataFrame(listRows3, schema3);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>
                (Map.of("sourceAlias1", dataset,"sourceAlias2",dataset2,"sourceAlias3",dataset3)));

        assertNotNull(datasetMap);
        assertEquals(1, datasetMap.size());

        Dataset<RowData> returnedDs = datasetMap.get("targetAlias1").as(Encoders.bean(RowData.class));


        final List<RowData> rows = datasetToTargetData(returnedDs, RowData.class);


    }


}