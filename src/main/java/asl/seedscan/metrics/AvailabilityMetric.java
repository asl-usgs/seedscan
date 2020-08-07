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

import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import asl.metadata.Channel;
import asl.metadata.meta_new.ChannelMeta;
import asl.seedsplitter.DataSet;

public class AvailabilityMetric extends Metric {
	private static final Logger logger = LoggerFactory
			.getLogger(asl.seedscan.metrics.AvailabilityMetric.class);

	@Override
	public long getVersion() {
		return 1;
	}

	@Override
	public String getName() {
		return "AvailabilityMetric";
	}

	public void process() {
		logger.info("-Enter- [ Station {} ] [ Day {} ]", getStation(), getDay());

		// Get a sorted list of continuous channels for this stationMeta and
		// loop over:

		List<Channel> channels = stationMeta.getContinuousChannels();

		for (Channel channel : channels) {

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

			metricResult.addResult(channel, result, digest);

		}// end foreach channel

	} // end process()

	@Override
	public String getSimpleDescription() {
		return "Returns a percentage of expected samples in the trace";
	}

	@Override
	public String getLongDescription() {
		return "For a sensor's sample rate and the length of a full day, this metric compares the "
				+ "number of points gotten from the data archive with the expected number of samples it "
				+ "should have. This is returned as a percentage; if we have 1 point when we expected 2, "
				+ "this would be a 50% availability result.";
	}

	private double computeMetric(Channel channel) {

		// AvailabilityMetric still returns a result (availability=0) even when
		// there is NO data for this channel
		if (!metricData.hasChannelData(channel)) {
			return 0.;
		}

		// Initialize availability and sample rates
		double availability;
		double metaSR;
		double dataSR;

		// The expected (=from metadata) number of samples:
		ChannelMeta chanMeta = stationMeta.getChannelMetadata(channel);
		final int SECONDS_PER_DAY = 86400;
		metaSR = chanMeta.getSampleRate();
		int expectedPoints = (int) (metaSR * SECONDS_PER_DAY + 1);
		// int expectedPoints = (int) (chanMeta.getSampleRate() * 24. * 60. *
		// 60.);

		// The actual (=from data) number of samples:
		List<DataSet> datasets = metricData.getChannelData(channel);

		int ndata = 0;

		for (DataSet dataset : datasets) {
			// Check sample rates of metadata and station channel data
			dataSR = dataset.getSampleRate();
			if (dataSR != metaSR) {
				logger.error(
						"SampleRate Mismatch: station:[{}] channel:[{}] day:[{}] "
								+ "metaSR:[{}] dataSR:[{}]", getStation(),
						channel, getDay(), metaSR, dataSR);
				continue;
			}
			ndata += dataset.getLength();
		} // end for each dataset

		if (expectedPoints > 0) {
			availability = 100. * ndata / expectedPoints;
		} else {
			logger.warn("Expected points for channel={} = 0!", channel);
			return NO_RESULT;
		}
		if (availability >= 101.00) {
			logger.warn(
					"Availability={} > 100%% for channel={} sRate={} day={}",
					availability, channel, chanMeta.getSampleRate(), getDay());
		}

		return availability;
	} // end computeMetric()
}
