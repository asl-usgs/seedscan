package asl.seedscan.metrics;

import asl.metadata.Channel;
import asl.metadata.meta_new.ChannelMeta;
import asl.metadata.meta_new.ResponseStage;
import asl.seedsplitter.DataSet;
import asl.util.Logging;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class MassPositionMetric.
 *
 * @author James Holland - USGS (jholland@usgs.gov)
 * @author Mike Hagerty
 * @author Alejandro Gonzalez - Honeywell
 * @author Joel Edwards - USGS
 */
public class VacuumMonitorMetric extends Metric {

  /**
   * The Constant logger.
   */
  private static final Logger logger = LoggerFactory
      .getLogger(asl.seedscan.metrics.VacuumMonitorMetric.class);

  VacuumMonitorMetric() {
    super();
    addArgument("channel-restriction");
  }

  /**
   * Computes the average of the vacuum monitor channel for the day.
   * <p>
   * Negative values should not occur. Counts greater than 26027 should not occur due to the voltage
   * limit on the sensors. These are not check for in the metric as the bad data is expected to
   * produce erratic results.
   *
   * @param channel the channel
   * @return the computed mass position value.
   * @throws MetricException thrown when a response has an invalid verification stage
   */
  private double computeMetric(Channel channel) throws MetricException {

    ChannelMeta chanMeta = this.stationMeta.getChannelMetadata(channel);
    List<DataSet> datasets = this.metricData.getChannelData(channel);

    ResponseStage stage0 = chanMeta.getStage(0);
    ResponseStage stage1 = chanMeta.getStage(1);
    ResponseStage stage2 = chanMeta.getStage(2);

    double totalGain = stage1.getStageGain() * stage2.getStageGain();

    //StageGain and the totalGain should be the same.
    if (Math.abs(1.0 - stage0.getStageGain() / totalGain) > 0.00001) {
      throw new MetricException(String.format(
          "Channel Response invalid: Verification stage mismatch: Verification= %s TotalGain=%s",
          stage0.getStageGain(), totalGain));
    }

    double totalPressure = 0;
    int totalDataPoints = 0;

    for (DataSet dataset : datasets) {
      int[] timeSeries = dataset.getSeries();
      for (int value : timeSeries) {
        totalPressure += value / totalGain;
      }
      totalDataPoints += dataset.getLength();
    }

    return totalPressure / totalDataPoints;
  }

  /**
   * @see asl.seedscan.metrics.Metric#getName()
   */
  @Override
  public String getName() {
    return "VacuumMonitorMetric";
  }

  /**
   * @see asl.seedscan.metrics.Metric#getVersion()
   */
  @Override
  public long getVersion() {
    return 1;
  }

  /**
   * @see asl.seedscan.metrics.Metric#process()
   */
  @Override
  public void process() {
    logger.info("-Enter- [ Station {} ] [ Day {} ]", this.getStation(), this.getDay());

    String bands = null;

    try {
      bands = get("channel-restriction");
    } catch (Exception ignored) {
    }

    if (bands == null) {
      bands = "VY";
    }

    // Get all VY? channels in metadata to use for loop
    List<Channel> channels = this.stationMeta.getChannelArray(bands, false, true);

    for (Channel channel : channels) {
      if (!this.metricData.hasChannelData(channel)) {
        logger.warn("No data found for channel:[{}] day:[{}] --> Skip metric", channel,
            this.getDay());
        continue;
      }

      ByteBuffer digest = this.metricData
          .valueDigestChanged(channel, this.createIdentifier(channel),
              this.getForceUpdate());

      if (digest == null) {
        logger.info("Digest unchanged station:[{}] channel:[{}] day:[{}] --> Skip metric",
            this.getStation(),
            channel, this.getDay());
        continue;
      }

      try {
        double result = this.computeMetric(channel);
        this.metricResult.addResult(channel, result, digest);
      } catch (MetricException e) {
        logger.error(Logging.prettyExceptionWithCause(e));
      }

    }
  }

  @Override
  public String getSimpleDescription() {
    return "Reports a sensor's mean pressure in kPa for detecting loss of vacuum over time";
  }

  @Override
  public String getLongDescription() {
    return "This metric takes the average value of a vacuum sensor (i.e., VY channels) over a "
        + "full-day's data, and normalizes that on the response gain to produce an average "
        + "pressure value in kPa. This value is intended to be interpreted over a long-term period "
        + "to determine if the sensor is experiencing a loss of vacuum over time.";
  }
}
