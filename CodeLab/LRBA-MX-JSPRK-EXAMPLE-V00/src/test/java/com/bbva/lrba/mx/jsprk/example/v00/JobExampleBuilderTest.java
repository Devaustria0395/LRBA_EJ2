package com.bbva.lrba.mx.jsprk.example.v00;

import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JobExampleBuilderTest {

    private JobExampleBuilder jobExampleBuilder;

    @BeforeEach
    void setUp() {
        this.jobExampleBuilder = new JobExampleBuilder();
    }

    @Test
    void registerSources_na_SourceList() {
        final SourcesList sourcesList = this.jobExampleBuilder.registerSources();
        assertNotNull(sourcesList);
        assertNotNull(sourcesList.getSources());
        assertEquals(3, sourcesList.getSources().size());

        final Source source = sourcesList.getSources().get(0);
        assertNotNull(source);
        assertEquals("sourceAlias1", source.getAlias());
        assertEquals("input1.csv", source.getPhysicalName());

        final Source source2 = sourcesList.getSources().get(1);
        assertNotNull(source2);
        assertEquals("sourceAlias2", source2.getAlias());
        assertEquals("input2.csv", source2.getPhysicalName());

        final Source source3 = sourcesList.getSources().get(2);
        assertNotNull(source3);
        assertEquals("sourceAlias3", source3.getAlias());
        assertEquals("input3.csv", source3.getPhysicalName());
    }

    @Test
    void registerTransform_na_Transform() {
        //IF YOU WANT TRANSFORM CLASS
        final TransformConfig transformConfig = this.jobExampleBuilder.registerTransform();
        assertNotNull(transformConfig);
        assertNotNull(transformConfig.getTransform());
        //IF YOU WANT SQL TRANSFORM
        //final TransformConfig transformConfig = this.jobExampleBuilder.registerTransform();
        //assertNotNull(transformConfig);
        //assertNotNull(transformConfig.getTransformSqls());
        //IF YOU DO NOT WANT TRANSFORM
        //final TransformConfig transformConfig = this.jobExampleBuilder.registerTransform();
        //assertNull(transformConfig);
    }

    @Test
    void registerTargets_na_TargetList() {
        final TargetsList targetsList = this.jobExampleBuilder.registerTargets();
        assertNotNull(targetsList);
        assertNotNull(targetsList.getTargets());
        assertEquals(2, targetsList.getTargets().size());

        final Target target = targetsList.getTargets().get(0);
        assertNotNull(target);
        assertEquals("joinDNI", target.getAlias());
        assertEquals("output/output.csv", target.getPhysicalName());

        final Target target2 = targetsList.getTargets().get(1);
        assertNotNull(target2);
        assertEquals("joinDNI3", target2.getAlias());
        assertEquals("output/output2.csv", target2.getPhysicalName());

    }

}