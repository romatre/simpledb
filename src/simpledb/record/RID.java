package simpledb.record;

/**
 * An identifier for a simpledb.record within a simpledb.file.
 * A RID consists of the block number in the simpledb.file,
 * and the ID of the simpledb.record in that block.
 * @author Edward Sciore
 */
public class RID {
	private int blknum;
	private int id;

	/**
	 * Creates a RID for the simpledb.record having the
	 * specified ID in the specified block.
	 * @param blknum the block number where the simpledb.record lives
	 * @param id the simpledb.record's ID
	 */
	public RID(int blknum, int id) {
		this.blknum = blknum;
		this.id     = id;
	}

	/**
	 * Returns the block number associated with this RID.
	 * @return the block number
	 */
	public int blockNumber() {
		return blknum;
	}

	/**
	 * Returns the ID associated with this RID.
	 * @return the ID
	 */
	public int id() {
		return id;
	}

	public boolean equals(Object obj) {
		RID r = (RID) obj;
		return blknum == r.blknum && id==r.id;
	}

	public String toString() {
		return "[" + blknum + ", " + id + "]";
	}
}
