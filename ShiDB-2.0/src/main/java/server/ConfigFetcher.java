package server;

import buffer.BufferSelectionStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import transaction.recovery.RecoveryMgrStrategy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Singleton config fetcher to make setting up the database simpler. Things like buffer size, block size and
 * other stuff will be in the config. This should make it much easier to create unit test setups since I only need
 * to define things here and can likely override them as needed
 */
public class ConfigFetcher {
    private static ConfigFetcher fetcherInstance;
    private static String configFilePath = "src/main/resources/config.json";

    @Getter
    private Map<String, Object> configMap;

    // On startup, need to read our config
    private ConfigFetcher() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            File configFile = Paths.get(configFilePath).toFile();

            configMap = mapper.readValue(configFile, Map.class);
        } catch (IOException e) {
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

    public static BufferSelectionStrategy getBufferMgrSelectionStrategy() {
        try {
            String configStrategy = getConfigs().configMap.get("buffer_mgr_selection_strategy").toString();
            return BufferSelectionStrategy.valueOf(configStrategy);
        } catch (IllegalArgumentException iae) {
            String validStrategies = "";
            for (BufferSelectionStrategy strategy : BufferSelectionStrategy.values())
                validStrategies += "\"" + strategy + "\" ";

            String configStrategy = getConfigs().configMap.get("buffer_mgr_selection_strategy").toString();
            String errorMsg = String.format("The \"buffer_mgr_selection_strategy\" field contains an unrecognized value: %s. Allowed values: %s", configStrategy, validStrategies);
            throw new IllegalArgumentException(errorMsg);
        } catch (NullPointerException npe) {
            return BufferSelectionStrategy.NAIVE;
        }
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

    // Ensure that this value is larger than the Buffer Mgr max wait time. If the buffer mgr is deadlocking on pinning
    // a buffer, it can cause the ConcurrencyMgr on top to also timeout. If the Concurrency Mgr times out first, it
    // will mask the real cause of the timeout
    public static long getConcurrencyMgrMaxWaitTime() {
        if (getConfigs().configMap.containsKey("concurrency_mgr_acquire_lock_max_wait_time_milliseconds"))
            return Long.parseLong(getConfigs().configMap.get("concurrency_mgr_acquire_lock_max_wait_time_milliseconds").toString());
        return 15000L; // return a default wait time of 15 seconds
    }

    public static long getConcurrencyMgrPollStepTime() {
        if (getConfigs().configMap.containsKey("concurrency_mgr_acquire_lock_poll_step_milliseconds"))
            return Long.parseLong(getConfigs().configMap.get("concurrency_mgr_acquire_lock_poll_step_milliseconds").toString());
        return 100L; // return a default wait time of 100 milliseconds
    }

    public static int getNumTransactionsPerCheckpoint() {
        if (getConfigs().configMap.containsKey("concurrency_mgr_checkpoint_every_n_transactions"))
            return Integer.parseInt(getConfigs().configMap.get("concurrency_mgr_checkpoint_every_n_transactions").toString());
        return 20; // return a default of checkpointing every 20 transactio ns
    }

    public static boolean useNQCheckpointing() {
        if (getConfigs().configMap.containsKey("concurrency_mgr_use_NQ_checkpoints"))
            return Boolean.parseBoolean(getConfigs().configMap.get("concurrency_mgr_use_NQ_checkpoints").toString());
        return false; // default to using quiescent checkpoints since they are the most basic
    }

    public static long getCheckpointingWaitPollStepTime() {
        if (getConfigs().configMap.containsKey("concurrency_mgr_poll_step_waiting_for_transaction_drain_q_checkpoint_milliseconds"))
            return Long.parseLong(getConfigs().configMap.get("concurrency_mgr_poll_step_waiting_for_transaction_drain_q_checkpoint_milliseconds").toString());
        return 100L; // return a default wait time of 100 milliseconds

    }

    public static RecoveryMgrStrategy getRecoveryMgrStrategy() {
        try {
            String configStrategy = getConfigs().configMap.get("recovery_manager_recovery_strategy").toString();
            return RecoveryMgrStrategy.valueOf(configStrategy);
        } catch (IllegalArgumentException iae) {
            String validStrategies = "";
            for (RecoveryMgrStrategy strategy : RecoveryMgrStrategy.values())
                validStrategies += "\"" + strategy + "\" ";

            String configStrategy = getConfigs().configMap.get("recovery_manager_recovery_strategy").toString();
            String errorMsg = String.format("The \"recovery_manager_recovery_strategy\" field contains an unrecognized value: %s. Allowed values: %s", configStrategy, validStrategies);
            throw new IllegalArgumentException(errorMsg);
        } catch (NullPointerException npe) {
            return RecoveryMgrStrategy.UNDO_ONLY; // Code default if no value is present in the config
        }
    }

    public static boolean isRecoveryStrategySimple() {
        try {
            String configStrategy = getConfigs().configMap.get("recovery_manager_recovery_strategy").toString();
            RecoveryMgrStrategy strategy = RecoveryMgrStrategy.valueOf(configStrategy);

            return strategy == RecoveryMgrStrategy.UNDO_ONLY || strategy == RecoveryMgrStrategy.REDO_ONLY;
        } catch (IllegalArgumentException iae) {
            String validStrategies = "";
            for (RecoveryMgrStrategy strategy : RecoveryMgrStrategy.values())
                validStrategies += "\"" + strategy + "\" ";

            String configStrategy = getConfigs().configMap.get("recovery_manager_recovery_strategy").toString();
            String errorMsg = String.format("The \"recovery_manager_recovery_strategy\" field contains an unrecognized value: %s. Allowed values: %s", configStrategy, validStrategies);
            throw new IllegalArgumentException(errorMsg);
        } catch (NullPointerException npe) {
            return true; // Since the code default is UNDO_ONLY, it's therefore, a simple strategy
        }
    }
}
