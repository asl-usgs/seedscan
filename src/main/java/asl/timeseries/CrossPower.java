package asl.timeseries;

import static asl.utils.NumericUtils.detrend;
import static asl.utils.NumericUtils.gapAwareDetrend;
import static asl.utils.timeseries.TimeSeriesUtils.ONE_HZ_INTERVAL;

import asl.metadata.Channel;
import asl.metadata.meta_new.ChannelMeta.ResponseUnits;
import asl.metadata.meta_new.ChannelMetaException;
import asl.seedscan.metrics.MetricData;
import asl.seedscan.metrics.MetricPSDException;
import asl.utils.FFTResult;
import java.util.Arrays;
import org.apache.commons.math3.complex.Complex;

public class CrossPower {

  private double[] powerSpectrum;
  private double[] frequencyArray;
  private double spectrumDeltaF;

  // constructor
  public CrossPower(double[] powerSpectrum, double df) {
    this.powerSpectrum = powerSpectrum;
    this.spectrumDeltaF = df;
  }

  /**
   * Use Peterson's algorithm (24 hrs = 13 segments with 75% overlap, etc.)
   *
   * @param channelX   - X-channel used for power-spectral-density computation
   * @param channelY   - Y-channel used for power-spectral-density computation
   * @param metricData - data to use as source of CrossPower computation
   * @throws ChannelMetaException the channel metadata exception
   * @throws MetricPSDException   the metric psd exception
   */
  public CrossPower(Channel channelX, Channel channelY, MetricData metricData)
      throws MetricPSDException, ChannelMetaException {
    // normally we would use the detrended padded full-day data method, but we get the data from
    // the datablock directly. this way, data that is at high sample rates (i.e, 100Hz)
    // can run without producing memory exceptions
    double[] dataX = metricData.getChannelData(channelX).getSampleRate() > 40 ?
        gapAwareDetrend(metricData.getChannelData(channelX)) :
        metricData.getDetrendedPaddedDayData(channelX);

    double[] dataY = metricData.getChannelData(channelY).getSampleRate() > 40 ?
        gapAwareDetrend(metricData.getChannelData(channelY)) :
        metricData.getDetrendedPaddedDayData(channelY);

    assert(dataX.length == dataY.length);

    doCrossPower(channelX, channelY, metricData, dataX, dataY);
  }

  /**
   * Perform the crosspower for a specific segment of data rather than the full day data by default.
   * Due to method visibility in MetricData the extraction of windowed data must be handled by the
   * metric.
   *
   * @param channelX   - X-channel used for power-spectral-density computation
   * @param channelY   - Y-channel used for power-spectral-density computation
   * @param metricData - data to use as source of CrossPower computation
   * @param xData      - trimmed data taken from channelX in metricData
   * @param yData      - trimmed data taken from channelY in metricData
   * @throws ChannelMetaException the channel metadata exception
   * @throws MetricPSDException   the metric psd exception
   */
  public CrossPower(Channel channelX, Channel channelY, MetricData metricData, double[] xData,
      double[] yData) throws MetricPSDException, ChannelMetaException {
      doCrossPower(channelX, channelY, metricData, xData, yData);
  }


  private void doCrossPower(Channel channelX, Channel channelY, MetricData metricData,
      double[] xData, double[] yData) throws MetricPSDException, ChannelMetaException {
    if (xData == null && yData == null && !channelX.equals(channelY)) {
      throw new MetricPSDException("Data for both channels (" + channelX.toString() +
          ", " + channelY.toString() + ") is null");
    } else if (xData == null) {
      throw new MetricPSDException("Data for first channel (" +
          channelX.toString() + ") was null for some reason");
    } else if (yData == null) {
      throw new MetricPSDException("Data for second channel (" +
          channelY.toString() + ") was null for some reason");
    }

    long interval = metricData.getChannelData(channelX).getInterval();

    if (interval != metricData.getChannelData(channelY).getInterval()) {
      throw new MetricPSDException("computePSD(): srateX (=" + interval + ") != srateY (="
          + metricData.getChannelData(channelY).getSampleRate() + ")\n");
    }

    if (interval == 0) {
      throw new MetricPSDException("Got srate=0");
    }
    FFTResult psdRaw = FFTResult.spectralCalc(xData, yData, interval);
    Complex[] spectrumRaw = psdRaw.getFFT();
    frequencyArray = psdRaw.getFreqs();

    this.spectrumDeltaF = psdRaw.getFreq(1);

    // Get the instrument response for Acceleration and remove it from the
    // PSD
    Complex[] instrumentResponseX = metricData.getMetaData().getChannelMetadata(channelX)
        .getResponse(frequencyArray, ResponseUnits.ACCELERATION);
    Complex[] instrumentResponseY = metricData.getMetaData().getChannelMetadata(channelY)
        .getResponse(frequencyArray, ResponseUnits.ACCELERATION);

    // Will hold the 1-sided PSD magnitude
    this.powerSpectrum = new double[frequencyArray.length];
    this.powerSpectrum[0] = 0;

    /*
     * We're computing the squared magnitude as we did with the FFT
     * above Start from k=1 to skip DC (k=0) where the response=0
     */
    for (int k = 1; k < frequencyArray.length; k++) {
      Complex responseMagnitude = instrumentResponseX[k]
          .multiply(instrumentResponseY[k].conjugate());
      if (responseMagnitude.abs() == 0) {
        throw new MetricPSDException("responseMagC[k]=0 --> divide by zero!\n");
      }
      // Divide out (squared)instrument response & Convert to dB:
      this.powerSpectrum[k] = spectrumRaw[k].divide(responseMagnitude).abs();
    }
  }

  public double[] getSpectrum() {
    return Arrays.copyOf(powerSpectrum, powerSpectrum.length);
  }

  public double getSpectrumDeltaF() {
    return spectrumDeltaF;
  }

  public double[] getSpectrumFrequencies() {
    return frequencyArray;
  }

}
