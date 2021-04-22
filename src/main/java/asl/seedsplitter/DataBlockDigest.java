package asl.seedsplitter;

import static java.util.Collections.sort;

import asl.security.MemberDigest;
import asl.utils.timeseries.DataBlock;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides MemberDigest wrapper for a DataBlock object.
 */
public class DataBlockDigest extends MemberDigest {

  private final DataBlock dataBlock;

  /**
   * Construct a new digest around a given datablock
   * @param db DataBlock to get digest of
   */
  public DataBlockDigest(DataBlock db) {
    this.dataBlock = db;
  }

  @Override
  protected void addDigestMembers() {
    addToDigest(dataBlock.getInitialInterval());
    //addToDigest(dataBlock.getStartTime());
    // always go in sorted order by times -- map/set iterators are not thread-consistent
    List<Long> times = new ArrayList<>(dataBlock.getDataMap().keySet());
    sort(times);
    for (long time : times) {
      addToDigest(time);
      double[] data = dataBlock.getDataMap().get(time);
      for (double point : data) {
        addToDigest(point);
      }
    }
  }

  /**
   * Get this digest object's underlying datablock.
   * @return DataBlock wrapped by this object
   */
  public DataBlock getDataBlock() {
    return dataBlock;
  }
}
