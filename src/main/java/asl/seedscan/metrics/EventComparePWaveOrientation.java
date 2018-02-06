package asl.seedscan.metrics;

import asl.freq.ButterworthFilter;
import asl.freq.SeisGramText;
import asl.metadata.Channel;
import asl.metadata.meta_new.StationMeta;
import asl.seedscan.event.EventCMT;
import asl.timeseries.TimeseriesUtils;
import edu.sc.seis.TauP.Arrival;
import edu.sc.seis.TauP.SphericalCoords;
import edu.sc.seis.TauP.TauModelException;
import edu.sc.seis.TauP.TauP_Time;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import org.apache.commons.math3.fitting.leastsquares.LeastSquaresBuilder;
import org.apache.commons.math3.fitting.leastsquares.LeastSquaresProblem;
import org.apache.commons.math3.fitting.leastsquares.MultivariateJacobianFunction;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sac.SacHeader;
import sac.SacTimeSeries;

public class EventComparePWaveOrientation extends Metric {

  private static final Logger logger =
      LoggerFactory.getLogger(asl.seedscan.metrics.EventComparePWaveOrientation.class);

  // length of window to take for data using synthetic surface comparison
  private static final int SURFACE_MS = 300000;

  // length of window to take for p-wave data
  private static final int P_WAVE_MS = 15000;

  // range of degrees (arc length) over which data will be valid
  private static final int MIN_DEGREES = 30;
  private static final int MAX_DEGREES = 90;

  public EventComparePWaveOrientation() {
    super();
    addArgument("base-channel");
    addArgument("channel-restriction");
  }

  @Override
  public long getVersion() {
    return 1;
  }

  @Override
  public String getName() {
    return "EventComparePWaveOrientation";
  }

  @Override
  public void process() {
    logger.info("-Enter- [ Station {} ] [ Day {} ]", getStation(), getDay());

    // a bunch of this is copy-pasted from eventCompareSynthetic since it's the same thing
    Hashtable<String, EventCMT> eventCMTs = getEventTable();
    if (eventCMTs == null) {
      logger.info(
          String.format("No Event CMTs found for Day=[%s] --> Skip EventComparePOrientation Metric",
              getDay()));
      return;
    }

    List<Channel> channels = stationMeta.getRotatableChannels();
    // get pairs of ED, ND data and then do the rotation with those
    String[] basechannel;
    String basePreSplit = null;

    try {
      basePreSplit = get("base-channel");

    } catch (NoSuchFieldException ignored) {
    }

    if (basePreSplit == null) {
      basePreSplit = "XX-LX";
      logger.info("No base channel for Event Compare P Orientation using: " + basePreSplit);
    }
    basechannel = basePreSplit.split("-");
    
    String preSplitBands = null;
    try {
      preSplitBands = get("channel-restriction");
    } catch (NoSuchFieldException ignored) {
    }
    if (preSplitBands == null) {
      preSplitBands = "LH";
      logger.info("No band restriction set for Event Compare P Orientation using: "
          + preSplitBands);
    }
    List<String> bands;
    bands = Arrays.asList(preSplitBands.split(","));


    StationMeta stnMeta = metricData.getMetaData();
    // get lat and long to make sure data is within a reasonable range
    double stnLat = stnMeta.getLatitude();
    double stnLon = stnMeta.getLongitude();

    // map used for quick access to paired channel data
    // we only do calculations once both N/E channel pairs (or ND/ED)
    // exist in the map
    Map<String, Channel> chNameMap = new HashMap<String, Channel>();
    for (Channel curChannel : channels) {

      String name = curChannel.toString();
      chNameMap.put(name, curChannel);
      String channelVal = name.split("-")[1];
      if (!bands.contains(channelVal.substring(0, 2))) {
        // current channel not part of valid band options, skip it
        continue;
      }

      int lastCharIdx = channelVal.length() - 1;
      char last = channelVal.charAt(lastCharIdx);
      if (last == 'Z' || last == 'E') {
        // assume vertical sensor component requires no orientation
        // assume east sensor will be referenced when north is read in
        continue;
      } else if (last == 'D') {
        if (channelVal.charAt(lastCharIdx - 1) == 'E') {
          // deal with, say, LHED (east-derived) when we read in LHND (north-derived)
          continue;
        }
      }

      String pairName;
      try {
        pairName = getPairedChannelNameString(name);
      } catch (MetricException e) {
        logger.error("Error in format of channel name (does not follow ENZ constraints): " + name);
        continue;
      }

      if (!chNameMap.keySet().contains(pairName)) {
        logger.warn("Could not find data for station with name " + pairName);
        continue;
      }

      // this is the function that produces rotated data given the current chanel
      ByteBuffer digest = metricData.valueDigestChanged(curChannel, createIdentifier(curChannel),
          getForceUpdate());
      
      Channel pairChannel = chNameMap.get(pairName);
      double sampleRateN = stationMeta.getChannelMetadata(curChannel).getSampleRate();
      double sampleRateE = stationMeta.getChannelMetadata(pairChannel).getSampleRate();
      // now curChannel, pairChannel the two channels to get the orientation of
      double azi = stationMeta.getChannelMetadata(curChannel).getAzimuth();
      // now we have the angle for which to rotate data

      // now to get the synthetics data
      SortedSet<String> eventKeys = new TreeSet<String>(eventCMTs.keySet());
      for (String key : eventKeys) {
        EventCMT eventMeta = eventCMTs.get(key);
        double evtLat = eventMeta.getLatitude();
        double evtLon = eventMeta.getLongitude();

        double angleBetween = SphericalCoords.distance(evtLat, evtLon, stnLat, stnLon);
        if (angleBetween < MIN_DEGREES || angleBetween > MAX_DEGREES) {
          logger.info("== {}: Arc length to key=[{}] out of range for this station\n",
              getName(), key);
          continue;
        }

        Hashtable<String, SacTimeSeries> synthetics = getEventSynthetics(key);
        if (synthetics == null) {
          logger.warn("== {}: No synthetics found for key=[{}] for this station\n", getName(), key);
          continue;
        }

        SacTimeSeries sacSynthetics = null;
        String fileKey = getStn() + "." + basechannel[0] + "." + basechannel[1].substring(0, 2)
            + channelVal.substring(2, 3) + ".modes.sac.proc";
        // e.g. "ANMO.XX.LXZ.modes.sac.proc"
        if (synthetics.containsKey(fileKey)) {
          sacSynthetics = synthetics.get(fileKey);
        } else {
          logger.warn("Did not find sac synthetic=[{}] in Hashtable", fileKey);
          continue; // Try next event
        }

        // get start time of p-wave, then take data 100 secs before that
        SacHeader header = sacSynthetics.getHeader();
        long eventStartTime = getSacStartTimeInMillis(header);
        long pTravelTime = eventStartTime + getPArrivalTime(eventMeta);
        
        long stationDataStartTime = pTravelTime;
        // ending of p-wave is this length of time afterward
        long stationEventEndTime = stationDataStartTime + P_WAVE_MS;
        // now, set window start back by 100 seconds (units in ms here)
        stationDataStartTime -= 100 * 1000; // 100 sec * 1000 ms/sec

        double[] northData = metricData.getWindowedData(curChannel, stationDataStartTime,
            stationEventEndTime);
        double[] eastData = metricData.getWindowedData(pairChannel, stationDataStartTime,
            stationEventEndTime);
        boolean somethingNull = false;
        if (null == northData) {
          logger.error("== {}: {} attempt to get north-facing data returned nothing",
              getName(), getStation());
          somethingNull = true;
        }
        // separate conditionals for each to log if one or both pieces of data not gettable
        if (null == eastData) {
          logger.error("== {}: {} attempt to get east-facing data returned nothing",
              getName(), getStation());
          somethingNull = true;
        }
        if (somethingNull) {
          continue;
        }
        if (northData.length != eastData.length) {
          logger.error("== {}: {} datasets of north & east not the same length!!",
              getName(), getStation());
          continue;
        }
        
        // preprocess (filter, detrend, normalize)
        // first, we low-pass filter the data
        SeisGramText sgt = new SeisGramText(""); // default english
        // low corner at 0Hz (don't highpass), high corner at 0.5Hz
        // and use a 4 poles in the filter
        ButterworthFilter bf = new ButterworthFilter(sgt, 0, 0.5, 4);
        // TODO: may need to do verification that this works how we expect
        northData = bf.apply(1 / sampleRateN, northData);
        eastData = bf.apply(1 / sampleRateE, eastData);

        // detrend operations are done in-place
        TimeseriesUtils.detrend(northData);
        TimeseriesUtils.detrend(eastData);
        // now normalize
        double minNorth = northData[0];
        double maxNorth = minNorth;
        double minEast = eastData[0];
        double maxEast = minEast;
        // get min and max values for scaling to (-1, 1)
        for (int i = 0; i < northData.length; ++i) {
          double testNorth = northData[i];
          double testEast = eastData[i];
          minNorth = Math.min(minNorth, testNorth);
          maxNorth = Math.max(maxNorth, testNorth);
          minEast = Math.min(minEast, testEast);
          maxEast = Math.max(maxEast, testEast);
        }
        // now scale data to (-1, 1)
        double eastChange = maxEast - minEast;
        double northChange = maxNorth - minNorth;
        for (int i = 0; i < northData.length; ++i) {
          double n = northData[i];
          double e = eastData[i];
          // scale to (0,2) then subtract 1 to get to (-1,1)
          northData[i] = (2 * (n - minNorth) / northChange) - 1;
          eastData[i] = (2 * (e - minEast) / eastChange) - 1;
        }
        // detrend operations are done in-place
        TimeseriesUtils.detrend(northData);
        TimeseriesUtils.detrend(eastData);
        

        // evaluate signal-to-noise ratio of data (RMS)
        // noise is first 15 seconds of data (i.e., before p-wave arrival)
        int noiseLength = getXSecondsLength(15, sampleRateN);
        // signal is last 15 seconds of data (i.e., after p-wave arrival);
        int signalOffset = northData.length - noiseLength;
        
        double sigN = 0., noiseN = 0., sigE = 0., noiseE = 0.;
        for (int i = 0; i < noiseLength; ++i) {
          sigN += Math.pow(northData[signalOffset + i], 2);
          noiseN += Math.pow(northData[i], 2);
          sigE += Math.pow(eastData[signalOffset + i], 2);
          noiseE += Math.pow(eastData[i], 2);
        }
        
        System.out.println("N: " + sigN + ", " + noiseN);
        
        sigN /= noiseLength;
        sigE /= noiseLength;
        noiseN /= noiseLength;
        noiseE /= noiseLength;
        
        sigN = Math.sqrt(sigN);
        sigE = Math.sqrt(sigE);
        noiseN = Math.sqrt(noiseN);
        noiseE = Math.sqrt(noiseE);
        
        double sigNoiseN = sigN / noiseN;
        double sigNoiseE = sigE / noiseE;
        final double SIGNAL_CUTOFF = 5.;
        System.out.println(sigNoiseN + ", " + sigNoiseE);
        if (sigNoiseN < SIGNAL_CUTOFF || sigNoiseE < SIGNAL_CUTOFF) {
          logger.warn("== {}: Signal to noise ratio under 5 -- [{} - {}], [{} - {}]", getName(),
              name, sigNoiseN, pairName, sigNoiseE);
          continue;
        }

        double sumNN = 0., sumEN = 0., sumEE = 0.;
        for (int i = 0; i < northData.length; ++i) {
          sumNN += northData[i] * northData[i];
          sumEE += eastData[i] * eastData[i];
          sumEN += eastData[i] * northData[i];
        }

        RealMatrix mat = new BlockRealMatrix(new double[][]{{sumNN, sumEN}, {sumEN, sumEE}});
        EigenDecomposition egd = new EigenDecomposition(mat);
        RealMatrix eigD = egd.getD();
        double linearity = (eigD.getEntry(2, 2) / eigD.getTrace()) -
            (eigD.getEntry(1, 1) / eigD.getTrace());
        if (linearity < 0.95) {
          logger.warn("== {}: Skipping; data linearity less than .95 -- [({} - {}) - {}]", 
              getName(), name, pairName, linearity);
          continue;
        }

        // TODO: need to extract the last range of data before this processing??
        double[] east = eastData; // change to arrays.copyofrange(eastdata...?
        double[] north = northData;

        // now do least squares on the data to get the linearity
        // and then do arctan solving on that to get the angle of orientation
        MultivariateJacobianFunction slopeCalculation = new MultivariateJacobianFunction() {
          public Pair<RealVector, RealMatrix> value(final RealVector point) {

            double slope = point.getEntry(0);
            RealVector value = new ArrayRealVector(1);
            for (int i = 0; i < east.length; ++i) {
              value.setEntry(i, (east[i] * slope));
            }

            // TODO: is an Array2DRowRealMatrix suitable for this?
            RealMatrix jacobian = new BlockRealMatrix(1, east.length);
            for (int i = 0; i < east.length; ++i) {
              jacobian.setEntry(0, i, slope);
            }

            return new Pair<RealVector, RealMatrix>(value, jacobian);

          }
        };

        LeastSquaresProblem lsp = new LeastSquaresBuilder().
            start(new double[]{0.}).
            target(north).
            model(slopeCalculation).
            lazyEvaluation(false).
            maxEvaluations(Integer.MAX_VALUE).
            maxIterations(Integer.MAX_VALUE).
            build();

        RealVector init = new ArrayRealVector(new double[]{0.});
        LeastSquaresProblem.Evaluation initEval = lsp.evaluate(init);
        // slope her
        double bAzim = Math.atan(initEval.getPoint().getEntry(0));
        bAzim = Math.toDegrees(bAzim);
        if (bAzim < 0) {
          bAzim = Math.abs(bAzim) + 90;
        }

        if (Math.abs(azi - (bAzim + 180)) < Math.abs(azi - bAzim)) {
          // recall that Java modulo permits negative numbers up to -(modulus)
          bAzim = ((bAzim + 180) % 360 + 360) % 360;
        }

        double angleDifference = Math.abs(bAzim - azi);

        // add warning before publishing result if it's inconsistent with expected
        if (angleDifference > 5) {
          StringBuilder sb = new StringBuilder();
          sb.append("== {}: Difference btwn calc. and est. azimuth > 5 degrees -- ");
          sb.append("[({} - {}) - ({} (calc) vs. {} (exp))]");
          logger.warn(sb.toString(), getName(), name, pairName, bAzim, azi);
        }

        // now, populate the results from this data
        metricResult.addResult(curChannel, angleDifference, digest);
        System.out.println("Result added");
      }
    }
  }

  private static int getXSecondsLength(int secs, double sRate) {
    // input is the sample rate in hertz
    // samples = 15 (s) * sRate (samp/s) 
    int count = (int) Math.ceil(secs * sRate);
    return count;
  }

  public static String getPairedChannelNameString(String channelName) throws MetricException {
    // get the string for the 
    
    char[] chNameArray = channelName.toCharArray();
    int lastCharIdx = chNameArray.length - 1;
    char lastChar = chNameArray[lastCharIdx];
    if (lastChar == 'D') {
      --lastCharIdx;
      lastChar = chNameArray[lastCharIdx];
    }
    char pairChar = getPairChar(lastChar);
    chNameArray[lastCharIdx] = pairChar;
    return new String(chNameArray);
  }

  private static char getPairChar(char orient) throws MetricException {
    // get N,E pairs (horizontal data) for orientation calculation
    if (orient == 'N') {
      return 'E';
    } else if (orient == 'E') {
      return 'N';
    } else {
      throw new MetricException("Error with channel orientation label format");
    }
  }

  private long getPArrivalTime(EventCMT eventCMT) {
    
    double evla = eventCMT.getLatitude();
    double evlo = eventCMT.getLongitude();
    double evdep = eventCMT.getDepth();
    double stla = stationMeta.getLatitude();
    double stlo = stationMeta.getLongitude();
    double gcarc = SphericalCoords.distance(evla, evlo, stla, stlo);
    double azim = SphericalCoords.azimuth(evla, evlo, stla, stlo);
    TauP_Time timeTool = null;
    try {
      timeTool = new TauP_Time("prem");
      timeTool.parsePhaseList("P");
      timeTool.depthCorrect(evdep);
      timeTool.calculate(gcarc);
    } catch (TauModelException e) {
      logger.error(e.getMessage());
      return -1; // Return null since arrival times are not
      // determinable.
    }

    List<Arrival> arrivals = timeTool.getArrivals();

    // We could screen by max distance (e.g., 90 deg for P direct)
    // or by counting arrivals (since you won't get a P arrival beyond about
    // 97 deg or so)
    if (arrivals.size() != 1) {
      // if we don't have P, or have more than P
      logger.info(String.format("Expected P arrival time not found [gcarc=%8.4f]", gcarc));
      return 0;
    }

    double arrivalTimeP = 0.;
    if (arrivals.get(0).getName().equals("P")) {
      arrivalTimeP = arrivals.get(0).getTime();
    } else {
      logger.info(String.format("Expected P arrival time not found"));
      // TODO: return 0 here too?
    }

    logger.info(String.format(
        "Event:%s <evla,evlo> = <%.2f, %.2f> Station:%s <%.2f, %.2f> gcarc=%.2f azim=%.2f tP=%.3f\n",
        eventCMT.getEventID(), evla, evlo, getStation(), stla, stlo, gcarc, azim, arrivalTimeP));

    return (long) arrivalTimeP * 1000; // get the arrival time in ms
  }

  /**
   * Gets the sac start time in millis.
   *
   * @param hdr the sac header
   * @return the sac start time in millis
   */
  private static long getSacStartTimeInMillis(SacHeader hdr) {
    GregorianCalendar gcal = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
    gcal.set(Calendar.YEAR, hdr.getNzyear());
    gcal.set(Calendar.DAY_OF_YEAR, hdr.getNzjday());
    gcal.set(Calendar.HOUR_OF_DAY, hdr.getNzhour());
    gcal.set(Calendar.MINUTE, hdr.getNzmin());
    gcal.set(Calendar.SECOND, hdr.getNzsec());
    gcal.set(Calendar.MILLISECOND, hdr.getNzmsec());

    return gcal.getTimeInMillis();
  }

}
