package asl.timeseries;

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

  private final double[] powerSpectrum;
  private double[] frequencyArray;
  private final double spectrumDeltaF;

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
    this(channelX, channelY, metricData, metricData.getDetrendedPaddedDayData(channelX),
        metricData.getDetrendedPaddedDayData(channelY));
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
      double[] yData)
      throws MetricPSDException, ChannelMetaException {

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

    double sampleRate = metricData.getChannelData(channelX).getSampleRate();

    if (sampleRate != metricData.getChannelData(channelY).getSampleRate()) {
      throw new MetricPSDException("computePSD(): srateX (=" + sampleRate + ") != srateY (="
          + metricData.getChannelData(channelY).getSampleRate() + ")\n");
    }

    if (sampleRate == 0) {
      throw new MetricPSDException("Got srate=0");
    }

    FFTResult psdRaw = FFTResult.spectralCalc(xData, yData,
        (long) (ONE_HZ_INTERVAL / sampleRate));
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
