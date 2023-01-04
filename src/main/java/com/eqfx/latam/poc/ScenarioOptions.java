package com.eqfx.latam.poc;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ScenarioOptions extends PipelineOptions {

    @Description("Scenario to run")
    @Validation.Required
    Scenario getScenario();
    void setScenario(Scenario scenario);

    @Description("source csv file")
    @Validation.Required
    String getSourceFile();
    void setSourceFile(String value);

    @Description("target avro file")
    @Validation.Required
    String getTargetFile();
    void setTargetFile(String value);

    enum Scenario {
        ONE,TWO
    }
}
