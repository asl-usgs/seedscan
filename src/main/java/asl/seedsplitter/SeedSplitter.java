package asl.seedsplitter;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import seed.Blockette320;

/**
 * @author Joel D. Edwards - USGS
 * @author Mike Hagerty
 * 
 *         The SeedSplitter class reads MiniSEED records from a list of files,
 *         filters out records that don't match the filters (if supplied),
 *         de-duplicates the data, orders it based on date, and breaks it up
 *         into DataSets based on continuity and station/channel info.
 */
public class SeedSplitter {
	private static final Logger logger = LoggerFactory
			.getLogger(asl.seedsplitter.SeedSplitter.class);

	private File[] m_files;
	private Hashtable<String, ArrayList<DataSet>> m_table;
	private LinkedBlockingQueue<ByteBlock> m_recordQueue;

	// MTH
	private Hashtable<String, ArrayList<Integer>> m_qualityTable;
	private Hashtable<String, ArrayList<Blockette320>> m_calTable;

	/**
	 * Hidden initializer which is called by all constructors.
	 * 
	 * @param fileList
	 *            List of files from which to read in the MiniSEED data.
	 */
	private void _construct(File[] fileList) {
		m_files = fileList;
		m_table = null;

		m_recordQueue = new LinkedBlockingQueue<>(1024);
	}

	/**
	 * Constructor.
	 * 
	 * @param fileList
	 *            List of files from which to read in the MiniSEED data.
	 */
	public SeedSplitter(File[] fileList) {
		super();
		_construct(fileList);
	}

	public Hashtable<String, ArrayList<Integer>> getQualityTable() {
		return m_qualityTable;
	}

	public Hashtable<String, ArrayList<Blockette320>> getCalTable() {
		return m_calTable;
	}

  /**
	 * Overrides the doInBackground method of SwingWorker, launching and
	 * monitoring two threads which read the files and process MiniSEED Data.
	 * 
	 * @return A hash table containing all of the data acquired from the file
	 *         list.
	 */
	public Hashtable<String, ArrayList<DataSet>> doInBackground() {
    boolean finalFile = false;

		SeedSplitProcessor processor = new SeedSplitProcessor(m_recordQueue);
		Thread processorThread = new Thread(processor);
		processorThread.start();
		for (int i = 0; i < m_files.length; i++) {
			if (i == (m_files.length - 1)) {
				finalFile = true;
			}
			File file = m_files[i];
			DataInputStream inputStream;
			Thread inputThread = null;
			try {
				inputStream = new DataInputStream(new BufferedInputStream(
						new FileInputStream(file)));
				SeedInputStream stream = new SeedInputStream(inputStream,
						m_recordQueue, finalFile);
				inputThread = new Thread(stream);
				logger.debug("Processing file " + file.getName() + "...");
				inputThread.start();
			} catch (FileNotFoundException e) {
				String message = "FileNotFoundException: File '"
						+ file.getName() + "' not found\n";
				logger.error(message, e);
				// Should we do something more? Throw an exception?
			}
			m_table = processor.getTable();
			m_qualityTable = processor.getQualityTable();
			m_calTable = processor.getCalTable();
			if (inputThread != null) {
				try {
					inputThread.join();
				} catch (InterruptedException e) {
					logger.error("InterruptedException:", e);
				}
			}
			if (finalFile) {
				try {
					processorThread.join();
				} catch (InterruptedException e) {
					logger.error("InterruptedException:", e);
				}
			}
    }
		return m_table;
	}
}
