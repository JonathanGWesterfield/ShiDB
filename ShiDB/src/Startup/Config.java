package Startup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;

import Buffer.Buffer;
import Constants.BufferMgrReplacementStrategies;

/**
 * Singleton instance to just fetch the configs on what modules to instantiate on startup.
 * This class is ugly as hell, but I just want an easy way to pull certain settings without
 * having to get into dependency injection and "the right way to do this". I only care about
 * learning about the database,
 */
public class Config {

    private static Config singletonConfigInstance;
    private Map<String, String> configMap;

    /**
     * Private Constructor. Instantiates a Jackson object mapper and reads the config.json
     * file in the resources folder.
     * @throws IOException
     */
    private Config() throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        File configFile = Paths.get("resources/config.json").toFile();

        configMap = jsonMapper.readValue(configFile, Map.class);
    }

    /**
     * Singleton getter instance.
     * @return Instance of this class
     */
    public static Config getConfigs() throws IOException {
        if (singletonConfigInstance == null) {
            synchronized (Config.class) {
                if (singletonConfigInstance == null)
                    singletonConfigInstance = new Config();
            }
        }

        return singletonConfigInstance;
    }

    /**
     * Gets the replacement strategy for the buffer manager from the config file. Options
     * to put in the config file are the defined in the {@link BufferMgrReplacementStrategies} file.
     * @return
     */
    public BufferMgrReplacementStrategies getBufferMgrReplacementStrategy() {
        return BufferMgrReplacementStrategies.valueOf(configMap.get("BufferMgrReplacementStrategies"));
    }
}
