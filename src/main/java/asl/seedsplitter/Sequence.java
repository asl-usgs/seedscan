package asl.seedsplitter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import asl.security.MemberDigest;

/**
 * The Class Sequence. Extends MemberDigest
 * 
 * Note: this class has a natural ordering that is inconsistent with equals.
 *
 * @author James Holland - USGS jholland@usgs.gov
 * @author Joel D. Edwards - USGS
 */
public class Sequence extends MemberDigest implements Comparable<Sequence>, Serializable {

	/**
	 * Serial Version UID
	 */
	private static final long serialVersionUID = 2L;

	/** The Constant logger. */
	private static final Logger logger = LoggerFactory.getLogger(asl.seedsplitter.Sequence.class);

	/** The Constant BLOCK_SIZE. */
	private static final int BLOCK_SIZE = 4096;

	/** The m_tz. */
	private static final TimeZone m_tz = TimeZone.getTimeZone("GMT");

	/** The m_pool. */
	private final BlockPool m_pool;

	/** The m_blocks. */
	private ArrayList<int[]> m_blocks = null;

	/**
	 * The m_block. This appears to be used as a temporary hold member and is
	 * thus transient.
	 */
	private transient int[] m_block = null;

	/** The m_length. */
	private int m_length = 0;

	/** The m_remainder. */
	private int m_remainder = 0;

	/** The m_start time. Microseconds since the epoch. */
	private long m_startTime = 0;

	/** The m_sample rate. */
	private double m_sampleRate = 0.0;

	/** The m_interval. */
	private long m_interval = 0;

	/**
	 * Creates a new instance of this object.
	 */
	public Sequence() {
		super();
		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
		m_pool = new BlockPool(BLOCK_SIZE);
		_reset();
	}

	/**
	 * Flushes all entries from the time series, but does not remove metadata.
	 */
	private void _reset() {
		m_length = 0;
		m_blocks = new ArrayList<>(8);
		this._addBlock();
	}

	/**
	 * Creates a new empty data block and adds it to the block list.
	 */
	private void _addBlock() {
		m_block = m_pool.getNewBlock();
		m_blocks.add(m_block);
		m_remainder = BLOCK_SIZE;
	}

	/**
	 * Adds start time of first data point and sample rate to digest. It then
	 * loops through blocks and adds them.
	 * 
	 */
	@Override
	protected void addDigestMembers() {
		addToDigest(m_startTime);
		addToDigest(m_sampleRate);
		int remaining = m_blocks.size();
		for (int[] block : m_blocks) {
			int numSamples = (--remaining > 0) ? BLOCK_SIZE : (BLOCK_SIZE - m_remainder);
			for (int i = 0; i < numSamples; i++) {
				addToDigest(block[i]);
			}
		}
	}

	/**
	 * Sets the timestamp of the first data point.
	 * 
	 * @param startTime
	 *            timestamp of first data point
	 */
	public void setStartTime(long startTime) {
		m_startTime = startTime;
	}

	/**
	 * Sets the time series' sample rate in Hz.
	 *
	 * @param sampleRate
	 *            sample rate in Hz
	 * @throws IllegalSampleRateException
	 *             the illegal sample rate exception
	 */
	public void setSampleRate(double sampleRate) throws IllegalSampleRateException {
		m_interval = sampleRateToInterval(sampleRate);
		m_sampleRate = sampleRate;
	}

  /**
	 * Extends the time-series by adding the specified data to the internal
	 * buffer.
	 * 
	 * @param buffer
	 *            The int array from which the data points should be copied.
	 * @param offset
	 *            Offset within buffer at which copying should begin.
	 * @param length
	 *            The number of elements to copy from
	 * 
	 * @throws IndexOutOfBoundsException
	 *             - if copying would cause access of data outside array bounds.
	 * @throws ArrayStoreException
	 *             - if an element in the buffer array could not be stored
	 *             because of a type mismatch.
	 * @throws NullPointerException
	 *             - if offset is null.
	 */
	public void extend(int[] buffer, int offset, int length) {
		int copySize;
		while (length > 0) {
			copySize = Math.min(m_remainder, length);
			System.arraycopy(buffer, offset, m_block, BLOCK_SIZE - m_remainder, copySize);
			if (m_remainder <= length) {
				this._addBlock();
			} else {
				m_remainder -= copySize;
			}
			offset += copySize;
			m_length += copySize;
			length -= copySize;
		}
	}

	/**
	 * Swaps the data contained within two Sequences such that they now contain
	 * each other's data. This is not a thread safe operation, and requires that
	 * all of the data components be synchronized correctly in order to prevent
	 * any being set while this operation is in progress.
	 * 
	 * @param seq
	 *            The Sequence with which this Sequence's data will be swapped.
	 */
	private void swapData(Sequence seq) {
		ArrayList<int[]> tempBlocks = m_blocks;
		int[] tempBlock = m_block;
		int tempLength = m_length;
		int tempRemainder = m_remainder;
		long tempStartTime = m_startTime;
		double tempSampleRate = m_sampleRate;
		long tempInterval = m_interval;

		m_blocks = seq.m_blocks;
		m_block = seq.m_block;
		m_length = seq.m_length;
		m_remainder = seq.m_remainder;
		m_startTime = seq.m_startTime;
		m_sampleRate = seq.m_sampleRate;
		m_interval = seq.m_interval;

		seq.m_blocks = tempBlocks;
		seq.m_block = tempBlock;
		seq.m_length = tempLength;
		seq.m_remainder = tempRemainder;
		seq.m_startTime = tempStartTime;
		seq.m_sampleRate = tempSampleRate;
		seq.m_interval = tempInterval;
	}

	/**
	 * Merges this Sequence into the target Sequence, removing all data points
	 * from this Sequence.
	 * 
	 * A SequenceIntervalMismatchException is thrown when the Sequences do not
	 * have the same interval.
	 * 
	 * A SequenceMergeRangeException is thrown when the Sequences are not
	 * contiguous, or do not overlap.
	 *
	 * @param seq
	 *            The Sequence into which the data points from this sequence
	 *            will be merged.
	 * @throws SequenceIntervalMismatchException
	 *             the sequence interval mismatch exception
	 * @throws SequenceMergeRangeException
	 *             the sequence merge range exception
	 * @throws BlockSizeMismatchException
	 *             the block size mismatch exception
	 */
	synchronized void mergeInto(Sequence seq) throws SequenceIntervalMismatchException, SequenceMergeRangeException,
			BlockSizeMismatchException {
		if (m_interval != seq.m_interval) {
			throw new SequenceIntervalMismatchException();
		}

		/*
		 * Allow for a fudge factor of 1 millisecond if sample rate is less than
		 * 100 Hz.
		 * 
		 * Is this a good idea, or would it be better to simply report a gap so
		 * the user is aware of the jump?
		 * 
		 * I changed this to have a fudge of 10 millisecond
		 */
		long intervalAdjustment = m_interval / 10;

		logger.debug("");
		logger.debug("    Source DataSet: " + Sequence.timestampToString(this.getStartTime()) + " to "
				+ Sequence.timestampToString(this.getEndTime()) + " (" + this.getLength() + " data points)");
		logger.debug("    Target DataSet: " + Sequence.timestampToString(seq.getStartTime()) + " to "
				+ Sequence.timestampToString(seq.getEndTime()) + " (" + seq.getLength() + " data points)");
		if (m_startTime > seq.getEndTime()) {
			if (((m_startTime - seq.getEndTime()) < (m_interval - intervalAdjustment))
					|| ((m_startTime - seq.getEndTime()) > (m_interval + intervalAdjustment))) {
				logger.warn("Source is more than 1 data point after target. (difference = "
						+ (m_startTime - seq.getEndTime()) + " ms OR " + ((m_startTime - seq.getEndTime()) / m_interval)
						+ " data points)");
				throw new SequenceMergeRangeException("Source is more than 1 data point after target.");
			}
		}

		if (this.getEndTime() < seq.m_startTime) {
			if (((seq.m_startTime - this.getEndTime()) < (m_interval - intervalAdjustment))
					|| ((seq.m_startTime - this.getEndTime()) > (m_interval + intervalAdjustment))) {
				logger.warn("Target is more than 1 data point after source. (difference = "
						+ (seq.m_startTime - this.getEndTime()) + ")");
				throw new SequenceMergeRangeException("Target is more than 1 data point after source.");
			} else {
				logger.debug("Swapping source and target prior to merge.");
				try {
					seq.mergeInto(this);
				} catch (BlockSizeMismatchException e) {
					logger.error("BlockSizeMismatchException:", e);
				}
				seq.swapData(this);
				return;
			}
		}

		if (this.subSequenceOf(seq)) {
			logger.debug("Subsequence. Just _reset().");
			// MTH: 2013-04-27 This seems wrong:
			// seq._reset();
			/*
			 * JH: 2016-12-02 Commented this out because assuming Sequence is a
			 * Dataset is risky. Afraid to delete because of Mike's comment.
			 * Hopefully this whole class will go away if I can migrate to using
			 * a different seed library. For now I am leaving the dead code in
			 * case I need to debug later.
			 */
			/*
			 * DataSet dataSet = (DataSet) this; logger.debug(
			 * "== Sequence.mergeInto() [{}_{} {}-{}] this=[{}-{}] is a subSequence of seq=[{}-{}] --> this._reset()\n"
			 * , dataSet.getNetwork(), dataSet.getStation(),
			 * dataSet.getLocation(), dataSet.getChannel(),
			 * Sequence.timestampToString(this.getStartTime()),
			 * Sequence.timestampToString(this.getEndTime()),
			 * Sequence.timestampToString(seq.getStartTime()),
			 * Sequence.timestampToString(seq.getEndTime()));
			 */
			this._reset();
			return;
		}

		if (m_startTime < seq.m_startTime) {
			logger.debug("Swapping source and target prior to merge.");
			try {
				seq.mergeInto(this);
			} catch (BlockSizeMismatchException e) {
				logger.error("BlockSizeMismatchException:", e);
			}
			seq.swapData(this);
			return;
		}

    int[] block;

		// We are going to flush the old data away through this process,
		// so let's do it now, and keep the old data around. This should
		// prevent others from messing with it while we are working.
		ArrayList<int[]> blocks = m_blocks;
		long startTime = m_startTime;
		long interval = m_interval;
		int remainder = m_remainder;
		this._reset();

		// skipCount is the number of data points we need to skip in order
		// to prevent a data overlap.
		int skipCount = 0;
		if (startTime <= seq.getEndTime()) {
			skipCount = (int) ((seq.getEndTime() - startTime) / interval + 1);
		}

		// In order to ensure no data overlaps, we need to burn of full
		// blocks that are in the overlapping range. Any remaining part
		// of a block that remains will be handled in the copy logic
		// below, which takes into account the remaining skipCount value.
		while (skipCount >= BLOCK_SIZE) {
			skipCount -= BLOCK_SIZE;
			block = blocks.remove(0);
		}

		int blockCount = blocks.size();
		int blockOffset;
		int blockLength;
		int copyLength;
		for (int i = 0; i < blockCount; i++) {
			block = blocks.remove(0);
			// If we end up on the last block, we need to update the block
			// length to
			// compensate for the skipped data points, and force the copyLength
			// computation below to be updated with this information.
			blockLength = BLOCK_SIZE - skipCount;
			// This automatically handles and updates the skipCount on the first
			// iteration
			// If skipCount is greater than one, we jump a way into the first
			// block,
			// after the first block, we always start at zero.
			blockOffset = skipCount;
			skipCount = 0;
			// If we have reached the last block, then remainder applies.
			if (i == (blockCount - 1)) {
				copyLength = blockLength - remainder;
			} else {
				copyLength = blockLength;
			}
			// Append the block's contents to the target Sequence
			if (copyLength > 0) {
				seq.extend(block, blockOffset, copyLength);
			}
			// Add the block to the target Sequence's BlockPool after its
			// contents have been copied.
			seq.m_pool.addBlock(block);
		}
	}

	/**
	 * Returns the timestamp of the first data point.
	 * 
	 * @return starting timestamp
	 */
	public long getStartTime() {
		return m_startTime;
	}

	/**
	 * Returns the timestamp of the last data point.
	 * 
	 * @return ending timestamp
	 */
	public long getEndTime() {
		return m_startTime + (m_interval * (m_length));
	}

	/**
	 * Returns the interval (in microseconds) based on the sample rate.
	 * 
	 * @return data point interval in microseconds
	 */
	public long getInterval() {
		return m_interval;
	}

	/**
	 * Returns the sample rate (in Hz) of the time series.
	 * 
	 * @return sample rate in Hz
	 */
	public double getSampleRate() {
		return m_sampleRate;
	}

	/**
	 * Returns the number of data points.
	 * 
	 * @return Returns the number of data points.
	 */
	public int getLength() {
		return m_length;
	}

	/**
	 * Returns a new Array containing a subset of the data points in this
	 * sequence.
	 *
	 * @param index
	 *            The index of the first returned data point.
	 * @param count
	 *            The number of data points to return.
	 * @return Returns a new Array containing a subset of the data points in
	 *         this sequence.
	 * @throws IndexOutOfBoundsException
	 *             the index out of bounds exception
	 * @throws SequenceRangeException
	 *             the sequence range exception
	 */
	private int[] getSeries(int index, int count) throws IndexOutOfBoundsException, SequenceRangeException {
		if (index >= m_length) {
			throw new IndexOutOfBoundsException();
		}
		if ((index + count) > m_length) {
			throw new SequenceRangeException();
		}
		if (m_length == 0) {
			return null;
		}

		int[] series = new int[count];
		int[] block;
		int numBlocks = m_blocks.size();
		int finalBlock = numBlocks - 1;
		int seriesLength = 0;

		int blockLength = BLOCK_SIZE;
		int burn = index / blockLength; // "burn off" this many blocks before
		// copying the first block
		int jump = index % blockLength; // start at this index within the first
		// block copied

		for (int i = burn; (i < numBlocks) && (count > 0); i++) {
			block = m_blocks.get(i);
			blockLength = BLOCK_SIZE - jump;
			if (i == finalBlock) {
				blockLength = blockLength - m_remainder;
			}
			if (blockLength > count) {
				blockLength = count;
			}
			System.arraycopy(block, jump, series, seriesLength, blockLength);
			seriesLength += blockLength;
			count -= blockLength;
			jump = 0;
		}

		return series;
	}

	/**
	 * Returns a new Array containing all of the data points in this sequence.
	 * 
	 * @return Returns a new Array containing all of the data points in this
	 *         sequence.
	 */
	public int[] getSeries() {
		try {
			return this.getSeries(0, m_length);
		} catch (IndexOutOfBoundsException e) {
			logger.error("IndexOutOfBoundsException:", e);
		} catch (SequenceRangeException e) {
			logger.error("RangeException:", e);
		}
		return null;
	}

	/**
	 * Returns an array of integer values that falls within the specified range.
	 * The specified start and end times must be within the range of the actual
	 * data.
	 *
	 * @param startTime
	 *            The first value should be at or after this point in time.
	 * @param endTime
	 *            The last value should be at or before this point in time.
	 * @return An array of int values.
	 * @throws SequenceRangeException
	 *             If the requested window is not contained within this
	 *             Sequence.
	 * @throws IndexOutOfBoundsException
	 *             the index out of bounds exception
	 */
	public int[] getSeries(long startTime, long endTime) throws SequenceRangeException, IndexOutOfBoundsException {
		int[] series;
		int index;
		int count;
		if (endTime > this.getEndTime()) {
			throw new SequenceRangeException();
		}
		if (startTime < m_startTime) {
			throw new SequenceRangeException();
		}
		count = (int) ((endTime - startTime) / m_interval);
		index = (int) (((startTime - m_startTime) + (m_interval / 2)) / m_interval);

		series = this.getSeries(index, count);
		return series;
	}

	/* comparison methods */
	/**
	 * Determines if this Sequence has a start time which is earlier than the
	 * start time of the supplied reference Sequence (may overlap).
	 * 
	 * @param seq
	 *            The reference Sequence with which to compare this Sequence.
	 * @return A boolean value; true if this Sequence starts before the start of
	 *         the reference Sequence, otherwise false.
	 */
	private boolean startsBefore(Sequence seq) {
		return (this.m_startTime < seq.m_startTime);
	}

	/**
	 * Determines if this Sequence has an end time which is later than the end
	 * time of the supplied reference Sequence (may overlap).
	 * 
	 * @param seq
	 *            The reference Sequence with which to compare this Sequence.
	 * @return A boolean value; true if this Sequence ends after the end time of
	 *         the reference Sequence, otherwise false.
	 */
	private boolean endsAfter(Sequence seq) {
		return (this.getEndTime() > seq.getEndTime());
	}

	/**
	 * Reports whether this Sequence contains data within the specified time
	 * range.
	 * 
	 * @param startTime
	 *            Start time for the test range.
	 * @param endTime
	 *            End time for the test range.
	 * @return A boolean value: true if this time range is available, otherwise
	 *         false.
	 */
	public boolean containsRange(long startTime, long endTime) {
		boolean result = false;
		if ((startTime >= this.m_startTime) && (endTime <= this.getEndTime())) {
			result = true;
		}
		return result;
	}

	/**
	 * Reports whether this Sequence is a sub-sequence of the supplied reference
	 * Sequence.
	 * 
	 * @param seq
	 *            The reference sequence with which to compare this Sequence.
	 * @return A boolean value: true if the range of this Sequence is within the
	 *         range of the reference Sequence.
	 * @throws SequenceIntervalMismatchException
	 *             if the reference Sequence's frequency does not match that of
	 *             this Sequence.
	 */
	private Boolean subSequenceOf(Sequence seq) throws SequenceIntervalMismatchException {
		if (seq.m_interval != m_interval) {
			throw new SequenceIntervalMismatchException();
		}
		return ((m_startTime >= seq.m_startTime) && (this.getEndTime() <= seq.getEndTime()));
	}

	/**
	 * Checks a sample rate to ensure it is valid, returning the associated
	 * interval in microseconds.
	 * 
	 * @param sampleRate
	 *            The sample rate to be verified and converted.
	 * @return A long integer value representing the supplied sample rate as an
	 *         interval.
	 * @throws IllegalSampleRateException
	 *             if the supplied sample rate is not one of the accepted
	 *             values.
	 */
	static long sampleRateToInterval(double sampleRate) throws IllegalSampleRateException {
	  if (sampleRate > 1000000){
	    throw new IllegalSampleRateException("Sample rate has exceeded 1 sample per microsecond.");
    }
    if (sampleRate < 3.17096999999e-8){
      throw new IllegalSampleRateException("Sample rate must be greater than 1 sample per year.");
    }
	  long interval =  (long) (1000000L / sampleRate);

    if (sampleRate >= 1.0) {
      return interval;
    }
    else {
      int scale = (int)(Math.log10(sampleRate)) - 2;
      return BigDecimal.valueOf(interval).setScale(scale, RoundingMode.HALF_UP).longValue();
    }
	}

	/**
	 * Converts a microsecond granualarity timestamp into a human readable
	 * string.
	 * 
	 * @param timestamp
	 *            - microseconds since the Epoch (January 1, 1970
	 *            00:00:00.000000 GMT)
	 * @return Returns the timestamp as a human readable string of the format
	 *         YYYY/MM/DD HH:MM:SS.mmmmmm
	 */
	static String timestampToString(long timestamp) {
		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
		String result;
		GregorianCalendar cal = new GregorianCalendar(m_tz);
		cal.setTimeInMillis(timestamp / 1000);
		result = String.format("%04d/%02d/%02d %02d:%02d:%02d.%06d", cal.get(Calendar.YEAR),
				cal.get(Calendar.MONTH) + 1, // MTH: Java uses months 0-11 ...
				cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE),
				cal.get(Calendar.SECOND), (cal.get(Calendar.MILLISECOND) * 1000 + (timestamp % 10)));
		return result;
	}

	/**
	 * Compare to another Sequence.
	 * 
	 * It compares the start and end times. If they all match then returns 0.
	 * The priority is with starts. If these match it compares end times.
	 *
	 * @param other
	 *            the sequence to compare to
	 * @return An integer value: 0 if Sequences are considered equal; less than
	 *         0 if this Sequence comes before Sequence other; greater than 0 if
	 *         Sequence other comes before this Sequence.
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(@NotNull Sequence other) {
		int result = 0;
		if (other == null) {
			result = 1;
		} else {
			if (this.startsBefore(other)) {
				result = -1;
			} else if (other.startsBefore(this)) {
				result = 1;
			} else if (other.endsAfter(this)) {
				result = -1;
			} else if (this.endsAfter(other)) {
				result = 1;
			}
		}
		return result;
	}
}
