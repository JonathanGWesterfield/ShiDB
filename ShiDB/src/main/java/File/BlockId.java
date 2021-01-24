package File;

public class BlockId {

    private String filename;
    private int blkNum;

    public BlockId(String filename, int blkNum) {
        this.filename = filename;
        this.blkNum = blkNum;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setBlkNum(int blkNum) {
        this.blkNum = blkNum;
    }

    public String getFilename() {
        return filename;
    }

    public int getBlkNum() {
        return blkNum;
    }

    public boolean equals(Object obj) {
        BlockId blk = (BlockId)obj;

        return blk.getFilename().equals(this.filename) && blk.getBlkNum() == this.blkNum;
    }

    public String toString() {
        return "[file: " + this.filename + ", block: " + this.blkNum + "]";
    }

    public int hashCode() {
        return this.toString().hashCode();
    }
}
