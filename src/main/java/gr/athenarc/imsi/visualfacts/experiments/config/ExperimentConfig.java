package gr.athenarc.imsi.visualfacts.experiments.config;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import gr.athenarc.imsi.visualfacts.Schema;

/**
 * Root configuration class for experiment YAML files.
 * Contains datasets and exploration scenarios.
 */
public class ExperimentConfig {

    @JsonProperty("datasets")
    private Map<String, DatasetConfig> datasets = new HashMap<>();

    @JsonProperty("scenarios")
    private Map<String, ExplorationScenarioConfig> scenarios = new HashMap<>();

    // Default constructor for Jackson
    public ExperimentConfig() {
    }

    /**
     * Gets a dataset configuration by name.
     */
    public DatasetConfig getDataset(String name) {
        return datasets.get(name);
    }

    /**
     * Gets a scenario configuration by name.
     */
    public ExplorationScenarioConfig getScenario(String name) {
        return scenarios.get(name);
    }

    /**
     * Converts a dataset configuration to a Schema object.
     */
    public Schema getSchemaForDataset(String datasetName) {
        DatasetConfig datasetConfig = datasets.get(datasetName);
        if (datasetConfig == null) {
            throw new IllegalArgumentException("Dataset not found: " + datasetName);
        }
        return datasetConfig.toSchema();
    }

    /**
     * Gets the Schema for a scenario's dataset.
     */
    public Schema getSchemaForScenario(String scenarioName) {
        ExplorationScenarioConfig scenarioConfig = scenarios.get(scenarioName);
        if (scenarioConfig == null) {
            throw new IllegalArgumentException("Scenario not found: " + scenarioName);
        }
        return getSchemaForDataset(scenarioConfig.getDataset());
    }

    // Getters and Setters

    public Map<String, DatasetConfig> getDatasets() {
        return datasets;
    }

    public void setDatasets(Map<String, DatasetConfig> datasets) {
        this.datasets = datasets;
    }

    public Map<String, ExplorationScenarioConfig> getScenarios() {
        return scenarios;
    }

    public void setScenarios(Map<String, ExplorationScenarioConfig> scenarios) {
        this.scenarios = scenarios;
    }

    @Override
    public String toString() {
        return "ExperimentConfig{" +
                "datasets=" + datasets.keySet() +
                ", scenarios=" + scenarios.keySet() +
                '}';
    }
}
