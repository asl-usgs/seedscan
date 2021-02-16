package asl.seedsplitter;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author Joel D. Edwards
 * 
 *         Keeps a pool of integer blocks of uniform size into which existing
 *         blocks can be injected in order to minimize the need for new
 *         allocations.
 */
public class BlockPool implements Serializable{
	/**
	 * Serial Version UID
	 */
	private static final long serialVersionUID = 1L;

	private final ArrayList<int[]> m_pool;
	private final int m_blockSize;

	/**
	 * Constructor.
	 * 
	 * @param blockSize
	 *            The size of each block within the pool.
	 */
	BlockPool(int blockSize) {
		m_blockSize = blockSize;
		m_pool = new ArrayList<>(8);
	}

  /**
	 * Adds a new block
	 * 
	 * @param block
	 *            The block to inject into the pool.
	 * @throws BlockSizeMismatchException
	 *             If the size of the added block does not match this
	 *             BlockPool's block size.
	 */
	void addBlock(int[] block) throws BlockSizeMismatchException {
		if (block.length != m_blockSize) {
			throw new BlockSizeMismatchException(
					"BlockSizeMismatchException: block.length != m_blockSize");
		}
		m_pool.add(block);
	}

	/**
	 * Returns a block from the pool if it contains any blocks, otherwise a new
	 * block is allocated.
	 * 
	 * @return A new block either from the pool, or freshly allocated if the
	 *         pool is empty.
	 */
	public synchronized int[] getNewBlock() {
		int[] block;
		if (m_pool.size() > 0) {
			block = m_pool.remove(0);
		} else {
			block = new int[m_blockSize];
		}
		return block;
	}
}
