package simpledb.stats;

public class BasicFileStats {

    private int blockRead = 0;
    private int blockWritten = 0;

    public int getBlockRead() {
        return blockRead;
    }

    public int getBlockWritten() {
        return blockWritten;
    }

    public void setBlockRead(int blockRead) {
        this.blockRead = blockRead;
    }

    public void setBlockWritten(int blockWritten) {
        this.blockWritten = blockWritten;
    }

    public void incrementBlockRead() {
        this.blockRead++;
    }

    public void incrementBlockWritten() {
        this.blockWritten++;
    }

}
