package asl.seedsplitter;

import edu.iris.dmc.seedcodec.CodecException;
import edu.sc.seis.seisFile.mseed.SeedFormatException;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.concurrent.LinkedBlockingQueue;

import asl.utils.timeseries.DataBlock;
import asl.utils.timeseries.TimeSeriesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seed.Blockette320;

/**
 * @author Joel D. Edwards - USGS
 * @author Mike Hagerty
 * <p>
 * The SeedSplitter class reads MiniSEED records from a list of files, filters out records that
 * don't match the filters (if supplied), de-duplicates the data, orders it based on date, and
 * creates datablocks that hold the relevant data
 */
public class SeedSplitter {

  private static final Logger logger = LoggerFactory
      .getLogger(asl.seedsplitter.SeedSplitter.class);

  private File[] m_files;
  private Hashtable<String, DataBlockDigest> m_table;
  /**
   * Hidden initializer which is called by all constructors.
   *
   * @param fileList List of files from which to read in the MiniSEED data.
   */
  private void _construct(File[] fileList) {
    m_files = fileList;
    m_table = null;
  }

  /**
   * Constructor.
   *
   * @param fileList List of files from which to read in the MiniSEED data.
   */
  public SeedSplitter(File[] fileList) {
    super();
    _construct(fileList);
  }

  /**
   * Overrides the doInBackground method of SwingWorker, launching and monitoring two threads which
   * read the files and process MiniSEED Data.
   *
   * @return A hash table containing all of the data acquired from the file list.
   */
  public Hashtable<String, DataBlockDigest> doInBackground() {
    m_table = new Hashtable<>();
    Arrays.stream(m_files).parallel().forEach(file -> {
      // we won't do a parallel inner loop as we expect files to in practice only have one channel
      try {
        for (String sncl : TimeSeriesUtils.getMplexNameList(file.getPath())) {
          DataBlock db = TimeSeriesUtils.getTimeSeries(file.getPath(), sncl);
          String[] components = db.getName().split("_");
          String key = components[2] + '-' + components[3];
          if (m_table.contains(key)) {
            m_table.get(key).getDataBlock().appendTimeSeries(db);
          } else {
            m_table.put(key, new DataBlockDigest(db));
          }
        }
      } catch (SeedFormatException e) {
        String message = "SeedFormatException: File '"
            + file.getName() + "' not a proper seed file\n";
        logger.error(message, e);
        // Should we do something more? Throw an exception?
      } catch (IOException e) {
        String message = "FileNotFoundException: File '"
            + file.getName() + "' not found\n";
        logger.error(message, e);
      } catch (CodecException e) {
        String message = "CodecException: File '"
            + file.getName() + "' is a valid seed file but has data issues\n";
        logger.error(message, e);
      }
    });

    return m_table;
  }
}
