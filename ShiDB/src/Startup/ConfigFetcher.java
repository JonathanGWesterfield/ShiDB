package Startup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import Constants.BufferMgrReplacementStrategies;

/**
 * Singleton instance to just fetch the configs on what modules to instantiate on startup.
 * This class is ugly as hell, but I just want an easy way to pull certain settings without
 * having to get into dependency injection and "the right way to do this". I only care about
 * learning about the database.
 */
public class ConfigFetcher {

    private static ConfigFetcher singletonConfigInstance;
    private Map<String, Object> configMap;

    /**
     * Private Constructor. Instantiates a Jackson object mapper and reads the config.json
     * file in the resources folder.
     * @throws IOException
     */
    private ConfigFetcher() throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        File configFile = Paths.get("resources/config.json").toFile();

        configMap = jsonMapper.readValue(configFile, Map.class);
    }

    /**
     * Singleton getter instance.
     * @return Instance of this class
     */
    public static ConfigFetcher getConfigs() {
        try {
            if (singletonConfigInstance == null) {
                synchronized (ConfigFetcher.class) {
                    if (singletonConfigInstance == null)
                        singletonConfigInstance = new ConfigFetcher();
                }
            }
        } catch (IOException e) {
            System.err.println(e);
        }

        return singletonConfigInstance;
    }

    /**
     * Gets the replacement strategy for the buffer manager from the config file. Options
     * to put in the config file are the defined in the {@link BufferMgrReplacementStrategies} file.
     * @return
     */
    public BufferMgrReplacementStrategies getBufferMgrReplacementStrategy() {
        return BufferMgrReplacementStrategies.valueOf(configMap.get("BufferMgrReplacementStrategies").toString());
    }

    /**
     * Gets the maximum time a client should wait for a buffer to be pinned before
     * the buffer manager throws a {@link Error.BufferAbortException}.
     * @return The maximum time the client should wait in milliseconds
     */
    public Long getBufferMgrPinWaitTime() {
        if (configMap.containsKey("BufferMgrPinWaitTime"))
            return Long.parseLong(configMap.get("BufferMgrPinWaitTime").toString());
        return 10000L; // return a default wait time of 10 seconds
    }

    /**
     * Get the number of buffers that will make up the buffer pool. Only used in
     * the startup of the whole system. Do not use for the {@link Buffer.BufferMgrTest}.
     * @return Number of buffers to instantiate the buffer pool with
     */
    public int getSizeOfBufferPool() {
        if (configMap.containsKey("SizeOfBufferPool"))
            return (int) configMap.get("SizeOfBufferPool");
        return 3; // same default as used in the ShiDB test
    }
}
