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
package asl.seedscan.metrics;

import asl.metadata.Channel;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimingQualityMetric extends Metric {

  private static final Logger logger = LoggerFactory
      .getLogger(asl.seedscan.metrics.TimingQualityMetric.class);

  @Override
  public long getVersion() {
    return 1;
  }

  @Override
  public String getName() {
    return "TimingQualityMetric";
  }

  @Override
  public String getUnitDescription() {
    return "timing quality";
  }

  public void process() {
    logger.info("-Enter- [ Station {} ] [ Day {} ]", getStation(), getDay());

    // Get a sorted list of continuous channels for this stationMeta and
    // loop over:
    List<Channel> channels = stationMeta.getContinuousChannels();

    for (Channel channel : channels) {
      if (!metricData.hasChannelData(channel)) {
        // logger.warn("No data found for channel[{}] --> Skip metric",
        // channel);
        continue;
      }

      ByteBuffer digest = metricData.valueDigestChanged(channel,
          createIdentifier(channel), getForceUpdate());

      if (digest == null) { // means oldDigest == newDigest and we don't
        // need to recompute the metric
        logger.info(
            "Digest unchanged station:[{}] channel:[{}] day:[{}] --> Skip metric",
            getStation(), channel, getDay());
        continue;
      }

      double result = computeMetric(channel);

      if (result == NO_RESULT) {
        // Do nothing --> skip to next channel
        logger.warn("NO_RESULT for station={} channel={} day={}",
            getStation(), channel, getDay());
      } else {
        metricResult.addResult(channel, result, digest);
      }
    }// end foreach channel
  } // end process()

  @Override
  public String getSimpleDescription() {
    return "Returns the average of the blockette 1001 timing records for the day";
  }

  @Override
  public String getLongDescription() {
    return "This metric takes the timing records for each blockette 1001 from a day's SEED data "
        + "and returns the average of these values.";
  }

  private double computeMetric(Channel channel) {

    if (!metricData.hasChannelData(channel)) {
      return NO_RESULT;
    }

    List<Integer> qualities = metricData.getChannelTimingQualityData(channel);

    if (qualities == null) {
      return NO_RESULT;
    }

    int totalQuality = 0;
    int totalPoints = 0;

    for (int i = 0; i < qualities.size(); i++) {
      totalQuality += qualities.get(i);
      totalPoints++;
    }

    double averageQuality = 0.;

    if (totalPoints > 0) {
      averageQuality = (double) totalQuality / (double) totalPoints;
    } else {
      logger.warn(
          "TimingQualityMetric: We have NO timing quality measurements for channel={} day={}",
          channel, getDay());
      return NO_RESULT;
    }

    return averageQuality;

  } // end computeMetric()
}
