package asl.seedscan.metrics;

import asl.metadata.Channel;
import asl.seedsplitter.DataSet;
import asl.util.Time;
import java.nio.ByteBuffer;
import java.util.List;

import asl.utils.timeseries.DataBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GapCountMetric extends Metric {

  private static final Logger logger = LoggerFactory
      .getLogger(asl.seedscan.metrics.GapCountMetric.class);

  @Override
  public long getVersion() {
    return 1;
  }

  @Override
  public String getName() {
    return "GapCountMetric";
  }

  @Override
  public String getUnitDescription() {
    return "gaps";
  }

  public void process() {
    logger.info("-Enter- [ Station {} ] [ Day {} ]", getStation(), getDay());

    String station = getStation();
    String day = getDay();
    String metric = getName();

    // Get a sorted list of continuous channels for this stationMeta and
    // loop over:
    List<Channel> channels = stationMeta.getContinuousChannels();

    for (Channel channel : channels) {
      if (!metricData.hasChannelData(channel)) {
        logger.warn(
            "No data found for station:[{}] channel:[{}] day:[{}] --> Skip metric",
            getStation(), channel, getDay());
        continue;
      }

      ByteBuffer digest = metricData.valueDigestChanged(channel,
          createIdentifier(channel), getForceUpdate());

      if (digest == null) { // means oldDigest == newDigest and we don't
        // need to recompute the metric
        logger.info(
            "Digest unchanged station:[{}] channel:[{}] day:[{}] --> Skip metric",
            getStation(), channel, day);
        continue;
      }

      double result = computeMetric(channel, station, day, metric);

      if (result == NO_RESULT) {
        // Do nothing --> skip to next channel
        logger.warn("NO_RESULT for station={} channel={} day={}",
            getStation(), channel, day);
      } else {
        metricResult.addResult(channel, result, digest);
      }
    }// end foreach channel
  } // end process()

  @Override
  public String getSimpleDescription() {
    return "Returns the number of gaps found between data records for a sensor's full-day trace.";
  }

  @Override
  public String getLongDescription() {
    return "This metric compares the start and end times of consecutive data records from a seed "
        + "file for a sensor's full-day data. If these records are more than a sample apart in "
        + "time, then that is counted as a gap. The total number of gaps found is reported. "
        + "Some metrics require a gapless trace, so if this value is positive, other metrics will "
        + "have an empty result.";
  }

  private double computeMetric(Channel channel, String station, String day,
      String metric) {

    // gap list is there in datablocks
    DataBlock datasets = metricData.getChannelData(channel);
    if (datasets == null) { // No data --> Skip this channel
      logger.error(
          "No datasets found for station=[{}] channel=[{}] day=[{}] --> Skip Metric",
          station, channel, day);
      return NO_RESULT;
    }

    // First count any interior gaps (= gaps that aren't at the
    // beginning/end of the day)
    int gapCount = datasets.getGapBoundaries().size();

    long firstSetStartTime = datasets.getStartTime(); // time in
    // microsecs
    // since
    // epoch
    long interval = datasets.getInterval(); // sample dt in microsecs

    long expectedStartTime = Time.calculateEpochMicroSeconds(stationMeta.getTimestamp());
    // double gapThreshold = interval / 2.;
    double gapThreshold = interval / 1.;

    // Check for possible gap at the beginning of the day
    if ((firstSetStartTime - expectedStartTime) > gapThreshold) {
      gapCount++;
    }

    long expectedEndTime = expectedStartTime + 86400000000L; // end of day
    // in
    // microsecs
    long lastSetEndTime = datasets.getEndTime();

    // Check for possible gap at the end of the day
    // We expect a full day to be 24:00:00 - one sample = (86400 - dt) secs
    if ((expectedEndTime - lastSetEndTime) > interval) {
      gapCount++;
    }

    return gapCount;
  } // end computeMetric()
}
