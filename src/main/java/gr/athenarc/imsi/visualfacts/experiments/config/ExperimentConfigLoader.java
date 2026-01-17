package gr.athenarc.imsi.visualfacts.experiments.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Utility class for loading experiment configuration from YAML files.
 * Supports loading from the classpath (default) or from an external file path.
 */
public class ExperimentConfigLoader {

    private static final Logger LOG = LogManager.getLogger(ExperimentConfigLoader.class);

    private static final String DEFAULT_CONFIG_PATH = "experiments/experiment_scenarios.yaml";
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    /**
     * Loads the experiment configuration from the default classpath location.
     * Default location: experiments/experiment_scenarios.yaml
     *
     * @return the loaded ExperimentConfig
     * @throws IOException if the configuration file cannot be read or parsed
     */
    public static ExperimentConfig loadDefault() throws IOException {
        LOG.info("Loading experiment config from classpath: {}", DEFAULT_CONFIG_PATH);
        try (InputStream inputStream = ExperimentConfigLoader.class.getClassLoader()
                .getResourceAsStream(DEFAULT_CONFIG_PATH)) {
            if (inputStream == null) {
                throw new IOException("Default config file not found in classpath: " + DEFAULT_CONFIG_PATH);
            }
            ExperimentConfig config = YAML_MAPPER.readValue(inputStream, ExperimentConfig.class);
            LOG.info("Loaded config with {} datasets and {} scenarios",
                    config.getDatasets().size(), config.getScenarios().size());
            return config;
        }
    }

    /**
     * Loads the experiment configuration from a specified file path.
     *
     * @param filePath the path to the YAML configuration file
     * @return the loaded ExperimentConfig
     * @throws IOException if the configuration file cannot be read or parsed
     */
    public static ExperimentConfig loadFromFile(String filePath) throws IOException {
        LOG.info("Loading experiment config from file: {}", filePath);
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("Config file not found: " + filePath);
        }
        ExperimentConfig config = YAML_MAPPER.readValue(file, ExperimentConfig.class);
        LOG.info("Loaded config with {} datasets and {} scenarios",
                config.getDatasets().size(), config.getScenarios().size());
        return config;
    }

    /**
     * Loads the experiment configuration from either a file path or the default classpath location.
     * If filePath is null or empty, loads from the default location.
     *
     * @param filePath optional path to the YAML configuration file (can be null or empty)
     * @return the loaded ExperimentConfig
     * @throws IOException if the configuration file cannot be read or parsed
     */
    public static ExperimentConfig load(String filePath) throws IOException {
        if (filePath == null || filePath.trim().isEmpty()) {
            return loadDefault();
        }
        return loadFromFile(filePath);
    }

    /**
     * Loads the experiment configuration from a specified classpath resource.
     * Useful for loading test configurations from test resources.
     *
     * @param classpathResource the classpath resource path (e.g., "experiments/test_scenarios.yaml")
     * @return the loaded ExperimentConfig
     * @throws IOException if the configuration file cannot be read or parsed
     */
    public static ExperimentConfig loadFromClasspath(String classpathResource) throws IOException {
        LOG.info("Loading experiment config from classpath: {}", classpathResource);
        try (InputStream inputStream = ExperimentConfigLoader.class.getClassLoader()
                .getResourceAsStream(classpathResource)) {
            if (inputStream == null) {
                throw new IOException("Config file not found in classpath: " + classpathResource);
            }
            ExperimentConfig config = YAML_MAPPER.readValue(inputStream, ExperimentConfig.class);
            LOG.info("Loaded config with {} datasets and {} scenarios",
                    config.getDatasets().size(), config.getScenarios().size());
            return config;
        }
    }
}
