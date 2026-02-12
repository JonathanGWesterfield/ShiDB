package server;

import lombok.Getter;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.core.JacksonException;

import java.io.File;

import java.nio.file.Paths;
import java.util.Map;

/**
 * Singleton config fetcher to make setting up the database simpler. Things like buffer size, block size and
 * other stuff will be in the config. This should make it much easier to create unit test setups since I only need
 * to define things here and can likely override them as needed
 */
public class ConfigFetcher {
    private static ConfigFetcher fetcherInstance;
    private static String configFilePath = "src/resources/config.json";

    @Getter
    private Map<String, Object> configMap;

    // On startup, need to read our config
    private ConfigFetcher() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            File configFile = Paths.get(configFilePath).toFile();

            configMap = mapper.readValue(configFile, Map.class);
        }
        catch(JacksonException e) {
            throw new RuntimeException("Failed to map the config file to Java Map! " + e.toString());
        }
    }

    public static synchronized ConfigFetcher getConfigs() {
        if (fetcherInstance == null)
            fetcherInstance = new ConfigFetcher();

        return fetcherInstance;
    }

    // Extra constructor to help load different config files depending on the needs of the unit test
    // This only let's me change the config on startup. I don't want to change configs on the fly
    public static synchronized ConfigFetcher getConfigs(String newConfigFilePath) {
        configFilePath = newConfigFilePath;

        if (fetcherInstance == null)
            fetcherInstance = new ConfigFetcher();

        return fetcherInstance;
    }

    public static int getDBFileBlockSize() {
        if (getConfigs().configMap.containsKey("db_file_block_size"))
            return (int) getConfigs().configMap.get("db_file_block_size");
        return 400; // same default as used in the ShiDB unit tests
    }

    public static long getBufferMgrMaxWaitTime() {
        if (getConfigs().configMap.containsKey("buffer_mgr_pin_max_wait_time_milliseconds"))
            return Long.parseLong(getConfigs().configMap.get("buffer_mgr_pin_max_wait_time_milliseconds").toString());
        return 10000L; // return a default wait time of 10 seconds
    }

    public static int getSizeOfBufferPool() {
        if (getConfigs().configMap.containsKey("size_of_buffer_pool"))
            return (int) getConfigs().configMap.get("size_of_buffer_pool");
        return 3; // same default as used in the ShiDB unit tests
    }

    public static long getBufferMgrPollStepTime() {
        if (getConfigs().configMap.containsKey("buffer_mgr_pin_poll_step_milliseconds"))
            return Long.parseLong(getConfigs().configMap.get("buffer_mgr_pin_poll_step_milliseconds").toString());
        return 100L; // return a default wait time of 100 milliseconds
    }
}
