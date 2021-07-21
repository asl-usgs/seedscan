package asl.seedscan.metrics;

import asl.metadata.Channel;
import asl.timeseries.CrossPower;
import asl.util.Logging;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pressure PSD metric for atmospheric sensors; this is the mean value over the .1Hz to .4Hz range.
 */
public class InfrasoundMetric extends Metric {

  /**
   * The Constant logger.
   */
  private static final Logger logger =
      LoggerFactory.getLogger(asl.seedscan.metrics.InfrasoundMetric.class);

  public InfrasoundMetric() {
    super();
    addArgument("channel-restriction");
  }

  @Override
  public long getVersion() {
    return 1;
  }

  @Override
  public String getName() {
    return "InfrasoundMetric";
  }

  @Override
  public String getUnitDescription() {
    return "pressure ((Pa)^2/Hz)";
  }

  @Override
  public void process() {
    logger.info("-Enter- [ Station {} ] [ Day {} ]", this.getStation(),
        this.getDay());

    String station = this.getStation();
    String day = this.getDay();
    String bands = null;

    try {
      bands = get("channel-restriction");

    } catch (Exception ignored) {
    }

    if (bands == null) {
      bands = "BD,HD";
    }

    // Get all VM? channels in metadata to use for loop
    List<Channel> channels = this.stationMeta.getChannelArray(bands,
        false, true);
    for (Channel channel : channels) {
      ByteBuffer digest = this.metricData.valueDigestChanged(channel,
          this.createIdentifier(channel), this.getForceUpdate());

      if (digest == null) { // means oldDigest == newDigest and we don't
        // need to recompute the metric
        logger.info(
            "Digest unchanged station:[{}] channel:[{}] day:[{}] --> Skip metric",
            this.getStation(), channel, day);
        continue;
      }

      try {
        double result = this.computeMetric(channel, station, day);

        this.metricResult.addResult(channel, result, digest);
      } catch (MetricException e) {
        logger.error(Logging.prettyExceptionWithCause(e));
      }
    }
  }

  @Override
  public String getSimpleDescription() {
    return "Gets the mean value of a pressure sensor's PSD over the .1-.4Hz range";
  }

  @Override
  public String getLongDescription() {
    return "This metric computes a full-day PSD for pressure sensors and returns the mean "
        + "value of the PSD over the 0.1Hz to 0.4Hz range.";
  }

  /**
   * Compute infrasound metric. Get the PSD over the range .1Hz to .4Hz and compute the mean value
   * in that range.
   *
   * @param channel the channel
   * @param station the station
   * @param day     the day
   * @return the double
   * @throws MetricException the metric exception
   */
  private double computeMetric(Channel channel, String station, String day)
      throws MetricException {

    // Compute/Get the 1-sided psd[f] using Peterson's algorithm (24 hrs, 13
    // segments, etc.)

    CrossPower crossPower = getCrossPower(channel, channel);
    double[] psd = crossPower.getSpectrum();
    double df = crossPower.getSpectrumDeltaF();
    int lowerBound = (int) (.1 / df); // index of .1Hz data
    int upperBound = (int) (.4 / df) + 1;
    if (upperBound >= psd.length || lowerBound >= psd.length) {
      throw new MetricException(String
          .format("station=[%s] channel=[%s] day=[%s]: We don't have enough information "
                  + "to get microbarom peak!--> Skip metric",
              station, channel.toString(), day));
    }
    int count = upperBound - lowerBound;
    double mean = 0;
    for (int i = lowerBound; i < upperBound; ++i) {
      mean += psd[i];
    }
    return mean / count;

  }
}
