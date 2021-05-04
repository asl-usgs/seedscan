package asl.seedsplitter;

import static java.util.Collections.sort;

import asl.seedscan.metrics.MetricException;
import asl.utils.timeseries.DataBlock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.math3.util.Pair;

/**
 * @author Joel D. Edwards
 * <p>
 * The BlockLocator class takes a ArrayList of {@code ArrayList<DataSet>} objects and builds a list
 * of contiguous data segments which are common across all of the {@code ArrayList<DataSet>}
 * objects.
 * <p>
 * This class used to extend SwingWorker, but this aspect was never actually used. If it is
 * determined that we want to make this runnable, we will want to convert to either a ScanWorker or
 * a basic Runnable.
 */
public class BlockLocator {

  /**
   * Searches for contiguous blocks of data across all of the supplied {@code ArrayList<DataSet>}
   * objects.
   *
   * @return A ArrayList of ContiguousBlock objects.
   */
  public static ArrayList<ContiguousBlock> buildBlockList(ArrayList<DataBlock> dataLists)
      throws MetricException {
    // The first ArrayList sets up the base ArrayList of ContiguousBlock
    // objects
    // Step through each of the remaining ArrayLists and build a new group
    // of
    // ContiguousBlock objects that contain a valid subset of the blocks
    // within the original ArrayList and the current data.

    ArrayList<ContiguousBlock> blockList = _buildFirstList(dataLists.get(0));
    for (DataBlock dataBlock : dataLists) {
      blockList = _buildDependentList(dataBlock, blockList);
    }

    return blockList;
  }

  /**
   * Generates the initial list of contiguous data regions.
   *
   * @param dataBlock A DataBlock object containing the data from a channel.
   * @return An ArrayList of ContiguousBlock objects.
   */
  private static ArrayList<ContiguousBlock> _buildFirstList(DataBlock dataBlock) {
    Map<Long, double[]> dataList = dataBlock.getDataMap();
    long interval = dataBlock.getInterval();

    ArrayList<ContiguousBlock> resultList = new ArrayList<>();
    ContiguousBlock tempBlock;

    for (long startTime : dataList.keySet()) {
      double[] data = dataList.get(startTime);
      long endTime = startTime + (data.length * interval);
      tempBlock = new ContiguousBlock(startTime, endTime, interval);
      resultList.add(tempBlock);
    }

    return resultList;
  }

  /**
   * Updates the list of contiguous data blocks based on the data in an additional data list.
   *
   * @param dataBlock  A timeseries data holder that will be used to update the contiguous data
   *                   blocks.
   * @param blockList The previous list of contiguous data blocks.
   * @return A new list of contiguous data blocks.
   * @throws MetricException If the sample rate of any of the DataSets does not match with those of
   *                         the ContiguousBlocks.
   */
  private static ArrayList<ContiguousBlock> _buildDependentList(
      DataBlock dataBlock, ArrayList<ContiguousBlock> blockList)
      throws MetricException {
    Set<ContiguousBlock> contiguousToConsider = new HashSet<>(blockList);
    ArrayList<ContiguousBlock> resultList = new ArrayList<>();
    long interval = dataBlock.getInitialInterval();
    long blockStart = dataBlock.getInitialStartTime();
    long blockEnd = dataBlock.getInitialEndTime();
    // first, exclude any blocks that don't fit within the start and end times of our datablock
    for (ContiguousBlock contiguous : blockList) {
      // this is a great time to check to make sure that there's no interval mismatch
      if (interval != contiguous.getInterval()) {
        throw new MetricException(String.format(
            "_buildDependentList: interval1=[%s] and/or interval2=[%s]",
            interval, contiguous.getInterval()));
      }

      if (contiguous.getEndTime() < blockStart || contiguous.getStartTime() > blockEnd) {
        contiguousToConsider.remove(contiguous);
      }
      else if (contiguous.getStartTime() < blockStart && contiguous.getEndTime() > blockStart) {
        contiguousToConsider.remove(contiguous);
        resultList.add(new ContiguousBlock(blockStart, contiguous.getEndTime(), interval));
      }
      else if (contiguous.getEndTime() > blockEnd && contiguous.getStartTime() < blockEnd) {
        contiguousToConsider.remove(contiguous);
        resultList.add(new ContiguousBlock(contiguous.getStartTime(), blockEnd, interval));
      }
    }

    outerLoop:
    for (ContiguousBlock contiguousBlock : contiguousToConsider) {
      // for each current contiguous block we need to ensure
      // 1. If this datablock has a gap during a contiguous block, split the contiguous block
      // along the gap (create two new contiguous blocks)
      // 2. If the contiguous block starts or ends during a gap, truncate the block accordingly
      // (we've already eliminated/fixed the blocks that are beyond this datablock's limits)
      // 3. If neither of these things are true, the contiguous block remains unchanged
      for (Pair<Long, Long> gap : dataBlock.getGapBoundaries()) {
        // a contiguous block may have multiple gaps intersecting it, so we change the contiguous
        // block as we iterate through the gaps, and add the bits before an implicit time cursor
        // based on the gaps' locations in time
        if (contiguousBlock.getStartTime() < gap.getFirst() &&
            contiguousBlock.getEndTime() > gap.getSecond()) {
          // add the time up to the given gap
          resultList.add(
              new ContiguousBlock(contiguousBlock.getStartTime(), gap.getFirst(), interval));
          // and modify the block under analysis to be the time after the gap
          contiguousBlock =
              new ContiguousBlock(gap.getSecond(), contiguousBlock.getEndTime(), interval);
        }
        // case 2 in the previous example where block starts during a gap
        else if (contiguousBlock.getStartTime() > gap.getFirst() &&
            contiguousBlock.getEndTime() < gap.getSecond()) {
          contiguousBlock =
              new ContiguousBlock(gap.getSecond(), contiguousBlock.getEndTime(), interval);
        }
        // case 2 where block ends during a gap -- in which case we're done with this block
        else if (contiguousBlock.getEndTime() > gap.getFirst() &&
            contiguousBlock.getEndTime() < gap.getSecond()) {
          resultList.add(
              new ContiguousBlock(contiguousBlock.getStartTime(), gap.getFirst(), interval));
          continue outerLoop; // time to look at the next contiguous block
        }
      }
      // if we've gone through all gaps and the block didn't end in one, we can add it to the list
      resultList.add(contiguousBlock);
    }

    return resultList;
  }
}
