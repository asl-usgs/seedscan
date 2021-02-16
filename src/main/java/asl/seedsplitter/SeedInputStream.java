package asl.seedsplitter;

import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import asl.seedscan.Global;
import seed.BlockSizeException;
import seed.IllegalSeednameException;
import seed.MiniSeed;

/**
 * @author Joel D. Edwards
 * 
 *         Reads MiniSEED records from multiple files, and pushes them into a
 *         queue to be processed by a supported class (seed
 *         {@link SeedSplitProcessor}).
 */
public class SeedInputStream implements Runnable {
	private static final Logger logger = LoggerFactory
			.getLogger(asl.seedsplitter.SeedInputStream.class);
	private static final Formatter formatter = new Formatter();

	private static final int MAX_RECORD_SIZE = 16384;
	private static final int BLOCK_SIZE = 256;

	private final DataInputStream m_inputStream;
	private final LinkedBlockingQueue<ByteBlock> m_queue;
  private final byte[] m_buffer;
	private int m_bufferBytes = 0;
  private final boolean m_indicateLast;
	private final String m_digest_algorithm = "MD5";
	private MessageDigest m_digest = null;

	/**
	 * Constructor.
	 * 
	 * @param inStream
	 *            The stream from which to read MiniSEED records.
	 * @param queue
	 *            The processing queue into which the MiniSEED records are
	 *            placed.
	 * @param indicateLast
	 *            An indicator of whether this is the last record for this
	 *            stream.
	 * @param disableDigest
	 *            A flag to disable assembling a digest of this stream's
	 *            contents.
	 */
	public SeedInputStream(DataInputStream inStream,
			LinkedBlockingQueue<ByteBlock> queue, boolean indicateLast,
			boolean disableDigest) {
		m_inputStream = inStream;
		m_queue = queue;
		m_buffer = new byte[MAX_RECORD_SIZE];
		m_indicateLast = indicateLast;
		if (!disableDigest) {
			try {
				m_digest = MessageDigest.getInstance(m_digest_algorithm);
			} catch (NoSuchAlgorithmException e) {
				logger.warn("NoSuchAlgorithmException:", e);
			}
		}
	}

	/**
	 * Constructor.
	 * 
	 * @param inStream
	 *            The stream from which to read MiniSEED records.
	 * @param queue
	 *            The processing queue into which the MiniSEED records are
	 *            placed.
	 * @param indicateLast
	 *            An indicator of whether this is the last record for this
	 *            stream.
	 */
	public SeedInputStream(DataInputStream inStream,
			LinkedBlockingQueue<ByteBlock> queue, boolean indicateLast) {
		this(inStream, queue, indicateLast, false);
	}

	/**
	 * Reads data from the input stream, assembles full SEED records and pushes
	 * them into the queue for processing.
	 */
	@Override
	public void run() {
		/*
		 * Read chunks of data in from input stream. Once a complete record has
		 * been assembled, put it into the queue for processing
		 */
    boolean m_running = true;
		int recordLength = -1;
		int bytesRead;
		int indicator;
		ByteBlock last = new ByteBlock(null, 0, true, true);
		ByteBlock end = new ByteBlock(null, 0, true, false);
		while (m_running) {
			try {
				if (m_bufferBytes < BLOCK_SIZE) {
					bytesRead = m_inputStream.read(m_buffer, m_bufferBytes,
							BLOCK_SIZE - m_bufferBytes);
					if (bytesRead < 0) {
						logger.debug("SeedInputStream Thread> I think we're done here...");
						if (m_indicateLast) {
							m_queue.put(last);
						} else {
							m_queue.put(end);
						}
						m_running = false;
						continue;
					}
					m_bufferBytes += bytesRead;

					// Update the contents of our SHA-1 digest.
					if (m_digest != null) {
						m_digest.update(Arrays.copyOfRange(m_buffer,
								m_bufferBytes, bytesRead));
					}

					if (m_bufferBytes == BLOCK_SIZE) {
						indicator = m_buffer[6] & 0xFF;

						//Restrict the data to the allowed quality flags. Typically 'D' 'M' 'Q' 'R' 
						String qualityFlagsStr = Global.getQualityflags();
						List<String> qualityFlags = Arrays.asList(qualityFlagsStr.split(","));
						if(
								  qualityFlags.contains("All") || 
								  qualityFlags.contains(String.valueOf((char)indicator)) // converts from int to String
						  )
						{
							try {
								recordLength = MiniSeed
										.crackBlockSize(m_buffer);
								if(recordLength == 0){
									System.out.println("No record Length");
								}
							} catch (IllegalSeednameException | BlockSizeException e) {
								logger.debug("Invalid Format, Skipping Chunk.");
								logger.error(e.getMessage());
                m_bufferBytes = 0;
							}
							/*
							 * firstBlockette = ((m_buffer[46] & 0xFF) << 8) |
							 * (m_buffer[47] & 0xFF); if (m_bufferBytes <=
							 * (firstBlockette + 6)) { logger.debug(
							 * "First blockette should be within the first 256 bytes"
							 * ); m_bufferBytes = 0; } else if
							 * ((((m_buffer[firstBlockette] & 0xFF) << 8) |
							 * (m_buffer[firstBlockette+1] & 0xFF)) != 1000) {
							 * logger.debug(
							 * "First record should be of type 1000, not type "
							 * + (m_buffer[firstBlockette] & 0xFF));
							 * m_bufferBytes = 0; } else { recordLength =
							 * (int)(java.lang.Math.pow(2,s
							 * m_buffer[firstBlockette + 6])); }
							 */
						}
						else
						{
              m_bufferBytes = 0;
							logger.error(formatter
									.format("Skipping bad indicator: 0x%x\n",
											indicator).toString());
						}
					}
				} else {
					m_bufferBytes += m_inputStream.read(m_buffer,
							m_bufferBytes, recordLength - m_bufferBytes);
					if (m_bufferBytes == recordLength) {
						m_queue.put(new ByteBlock(m_buffer, recordLength
            ));
						m_bufferBytes = 0;
          }
				}
			} catch (IOException e) {
				logger.error("IOException:", e);
			} catch (InterruptedException e) {
				logger.error("InterruptedException:", e);
			}
		}
	}
}
