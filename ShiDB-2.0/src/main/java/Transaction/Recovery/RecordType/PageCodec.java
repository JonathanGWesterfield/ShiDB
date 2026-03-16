package Transaction.Recovery.RecordType;

import File.Page;

public interface PageCodec<T> {
    T read(Page page, int offset);
    void write(Page page, int offset, T value);
    int byteSize(T value); // Strings and date time need this; primitives are consistent
}
