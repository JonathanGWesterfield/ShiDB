package Buffer;

public enum BufferSelectionStrategy {
    NAIVE, // Choose the first unpinned buffer it finds
    FIFO, // Choose the unpinned buffer whose contents were unpinned least recently (pinned -> lowest sys time)
    LRU, // Choose the unpinned buffer whose contents were unpinned least recently (unpinned -> lowest syst time)
    RING_BUFFER; // Scan buffers sequentially  from last replaced buffer; choose first unpinned buffer
}
