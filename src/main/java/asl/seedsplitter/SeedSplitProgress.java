/*
 * Copyright 2012, United States Geological Survey or
 * third-party contributors as indicated by the @author tags.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/  >.
 *
 */
package asl.seedsplitter;

import asl.worker.Progress;

/**
 * @author Joel D. Edwards
 * 
 *         Stores progress information about the progress made by
 *         {@link SeedSplitter}.
 */
public class SeedSplitProgress implements Progress {
	private long m_byteCount;

	private SeedSplitError m_errorType = SeedSplitError.NONE;
	private String m_errorMessage;

	private double m_percentComplete;
	private boolean m_complete;
	private boolean m_fileDone;

	/**
	 * Constructor.
	 * 
	 * @param byteCount
	 *            The number of bytes that have been read to this point.
	 */
	public SeedSplitProgress(long byteCount) {
		_init(byteCount, false, SeedSplitError.NONE, null);
	}

	/**
	 * Hidden initialization method called by all constructors.
	 * 
	 * @param byteCount
	 *            The number of bytes that have been read to this point.
	 * @param complete
	 *            A boolean value set to true if all data has been read.
	 * @param errorType
	 *            The type of error that occurred, if any.
	 * @param errorMessage
	 *            A message describing the error if one occurred.
	 */
	private void _init(long byteCount, boolean complete,
			SeedSplitError errorType, String errorMessage) {
		switch (errorType) {
		case NONE:
			m_errorMessage = null;
			break;
		case SEED_SIGNATURE_CHANGED:
		case SAMPLE_RATE_CHANGED:
			m_errorMessage = errorType
					+ ": "
					+ ((errorMessage == null) ? "No Error Information"
							: errorMessage);
			break;
		}
		m_errorType = errorType;
		m_byteCount = byteCount;
		m_complete = complete;
	}

	/**
	 * Sets the completion status of the file read operation.
	 * 
	 * @param done
	 *            Set to true if the file read operation is complete.
	 */
	public void setFileDone(boolean done) {
		m_fileDone = done;
	}

  /**
	 * Indicates whether processing is complete.
	 * 
	 * @return True if all processing is complete; otherwise false.
	 */
	public boolean isComplete() {
		return m_complete;
	}

	/**
	 * Indicates whether an error occurred.
	 * 
	 * @return True if an error occurred; otherwise false.
	 */
	public boolean errorOccurred() {
		return m_errorType != SeedSplitError.NONE;
	}

	/**
	 * Reports the type of error that occurred, if any.
	 * 
	 * @return A SeedSplitError enumerated type indicating the type of erro that
	 *         occurred, if any.
	 */
	public SeedSplitError getErrorType() {
		return m_errorType;
	}

	/**
	 * Reports the error message associated with the error, if one occurred.
	 * 
	 * @return A String with a description of the error, if one occurred.
	 */
	public String getErrorMessage() {
		return m_errorMessage;
	}

	/**
	 * Reports the current progress percent.
	 *
	 * @return A double value between 0.0 and 1.0 indicating the current
	 *         progress percent.
	 */
	public double getProgressPercent() {
		return m_percentComplete;
	}
}
