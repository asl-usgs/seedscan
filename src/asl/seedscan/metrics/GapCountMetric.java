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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

import java.nio.ByteBuffer;
import asl.util.Hex;

import asl.metadata.Channel;
import asl.metadata.meta_new.ChannelMeta;
import asl.seedsplitter.DataSet;

public class GapCountMetric
extends Metric
{
    private static final Logger logger = LoggerFactory.getLogger(asl.seedscan.metrics.GapCountMetric.class);

    @Override public long getVersion()
    {
        return 1;
    }

    @Override public String getName()
    {
        return "GapCountMetric";
    }


    public void process()
    {
        logger.info("-Enter- [ Station {} ] [ Day {} ]", getStation(), getDay());

    // Get a sorted list of continuous channels for this stationMeta and loop over:
        List<Channel> channels = stationMeta.getContinuousChannels();

        for (Channel channel : channels){

            if (!metricData.hasChannelData(channel)){
                //logger.warn("No data found for channel[{}] --> Skip metric", channel);
                continue;
            }

            ByteBuffer digest = metricData.valueDigestChanged(channel, createIdentifier(channel), getForceUpdate());

            if (digest == null) { // means oldDigest == newDigest and we don't need to recompute the metric
                logger.warn("Digest unchanged station:[{}] channel:[{}] --> Skip metric", getStation(), channel);
                continue;
            }

            double result = computeMetric(channel);

            if (result == NO_RESULT) {
                // Do nothing --> skip to next channel
                logger.warn("NO_RESULT for station={} channel={}", getStation(), channel);
            }
            else {
                metricResult.addResult(channel, result, digest);
            }

        }// end foreach channel
    } // end process()


    private double computeMetric(Channel channel) {

        List<DataSet>datasets = metricData.getChannelData(channel);
        if (datasets == null) {  // No data --> Skip this channel
            logger.error("No datasets found for channel=[" + channel + "] --> Skip Metric");
            return NO_RESULT;
        }

     // First count any interior gaps (= gaps that aren't at the beginning/end of the day)
        int gapCount = datasets.size()-1;

        long firstSetStartTime = datasets.get(0).getStartTime();  // time in microsecs since epoch
        long interval          = datasets.get(0).getInterval();   // sample dt in microsecs

     // stationMeta.getTimestamp() returns a Calendar object for the expected day
     //   convert it from milisecs to microsecs
        long expectedStartTime = stationMeta.getTimestamp().getTimeInMillis() * 1000;
        //double gapThreshold = interval / 2.;
        double gapThreshold = interval / 1.;
        
     // Check for possible gap at the beginning of the day
        if ((firstSetStartTime - expectedStartTime) > gapThreshold) {
            gapCount++;
        }

        long expectedEndTime = expectedStartTime + 86400000000L;  // end of day in microsecs
        long lastSetEndTime  = datasets.get(datasets.size()-1).getEndTime(); 

     // Check for possible gap at the end of the day
     // We expect a full day to be 24:00:00 - one sample = (86400 - dt) secs 
        if ((expectedEndTime - lastSetEndTime) > interval) {
            gapCount++;
        }

        return (double)gapCount;

    } // end computeMetric()

}
