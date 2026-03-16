package Server;

import Buffer.BufferSelectionStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.jetbrains.annotations.TestOnly;
import Transaction.Recovery.RecoveryMgrStrategy;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

    // Some unit test need to reload configs... so here we are...
    @TestOnly
    public static synchronized void reloadConfig(String newConfigFilePath) {
        fetcherInstance = null;
        getConfigs(newConfigFilePath);
    }

    public static synchronized ConfigFetcher getConfigs() {
        if (fetcherInstance == null)
            fetcherInstance = new ConfigFetcher();

        return fetcherInstance;
    }

    // Extra constructor to help load different config files depending on the needs of the unit test
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

    public static String getDBFileDirectory() {
        if (getConfigs().configMap.containsKey("database_directory"))
            return getConfigs().configMap.get("database_directory").toString();
        return "ShiDB-Dir"; // I have no idea where this will end up, but at least I know it's name...
    }

    public static String getDBLogFileName() {
        if (getConfigs().configMap.containsKey("database_log_file"))
            return getConfigs().configMap.get("database_log_file").toString();
        return "shidb-2.0.log"; // I have no idea where this will end up, but at least I know it's name...
    }

    public static Charset getStringCharset() {
        final Map<String, Charset> CHARSET_MAP = Map.of(
                "US_ASCII",  StandardCharsets.US_ASCII,
                "UTF_8",     StandardCharsets.UTF_8,
                "UTF_16",    StandardCharsets.UTF_16,
                "UTF_32",    StandardCharsets.UTF_32
        );
        if (getConfigs().configMap.containsKey("string_charset")) {
            String strCharset = getConfigs().configMap.get("string_charset").toString();

            if (!CHARSET_MAP.containsKey(strCharset))
                throw new IllegalArgumentException("Unknown charset for string encoding: " + strCharset + "!");

            return CHARSET_MAP.get(strCharset);
        }

        return StandardCharsets.US_ASCII;
    }

    public static int getDBFileBlockSize() {
        if (getConfigs().configMap.containsKey("db_file_block_size_bytes"))
            return (int) getConfigs().configMap.get("db_file_block_size_bytes");
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
    public static long getConcurrencyMgrMaxWaitTimeNano() {
        if (getConfigs().configMap.containsKey("concurrency_mgr_acquire_lock_max_wait_time_milliseconds")) {
            long millis = Long.parseLong(getConfigs().configMap.get("concurrency_mgr_acquire_lock_max_wait_time_milliseconds").toString());
            return millis * 1_000_000L;
        }
        return 15000L * 1_000_000L; // return a default wait time of 15 seconds
    }

    public static long getConcurrencyMgrPollStepTimeNano() {
        if (getConfigs().configMap.containsKey("concurrency_mgr_acquire_lock_poll_step_milliseconds")) {
            long millis = Long.parseLong(getConfigs().configMap.get("concurrency_mgr_acquire_lock_poll_step_milliseconds").toString());
            return millis * 1_000_000L;
        }
        return 100L * 1_000_000L; // return a default wait time of 100 milliseconds
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
