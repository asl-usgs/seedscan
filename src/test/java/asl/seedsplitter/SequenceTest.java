package asl.seedsplitter;

import static asl.seedsplitter.Sequence.sampleRateToInterval;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

public class SequenceTest {

  @Test
  public void testSampleRateToInterval_previous_hardcoded_values()
      throws IllegalSampleRateException {
    Assert.assertEquals(1000000000L, sampleRateToInterval(0.001));
    Assert.assertEquals(100000000L, sampleRateToInterval(0.01));
    Assert.assertEquals(10000000L, sampleRateToInterval(0.1));
    Assert.assertEquals(1000000L, sampleRateToInterval(1.0));
    Assert.assertEquals(400000L, sampleRateToInterval(2.5));
    Assert.assertEquals(250000L, sampleRateToInterval(4.0));
    Assert.assertEquals(200000L, sampleRateToInterval(5.0));
    Assert.assertEquals(100000L, sampleRateToInterval(10.0));
    Assert.assertEquals(50000L, sampleRateToInterval(20.0));
    Assert.assertEquals(25000L, sampleRateToInterval(40.0));
    Assert.assertEquals(20000L, sampleRateToInterval(50.0));
    Assert.assertEquals(10000L, sampleRateToInterval(100.0));
    Assert.assertEquals(5000L, sampleRateToInterval(200.0));
    Assert.assertEquals(4000L, sampleRateToInterval(250.0));
    Assert.assertEquals(2500L, sampleRateToInterval(400.0));
    Assert.assertEquals(2000L, sampleRateToInterval(500.0));
    Assert.assertEquals(1000L, sampleRateToInterval(1000.0));
    Assert.assertEquals(500L, sampleRateToInterval(2000.0));
    Assert.assertEquals(250L, sampleRateToInterval(4000.0));
    Assert.assertEquals(200L, sampleRateToInterval(5000.0));
  }

  @Test
  public void testSampleRateToInterval_one_sample_per_year_irreducible_origin()
      throws IllegalSampleRateException {
    // 1/31536000 = 1 sample per year ~= 3.1709791983764586504312531709791983764586504312531709791983 x 10^-8
    // 9 off of pure math
    Assert.assertEquals(31536000000000L, sampleRateToInterval(3.17097e-8));
  }

  @Test
  public void testSampleRateToInterval_two_samples_per_year_irreducible_origin()
      throws IllegalSampleRateException {
    // 1/15768000 = 2 samples per year ~= 6.3419583967529173008625063419583967529173008625063419583967 x 10^-8
    //7,8, or 9 off of pure math
    Assert.assertEquals(15768000000000L, sampleRateToInterval(6.34196e-8));
  }

  @Test
  public void testSampleRateToInterval_one_sample_per_month_irreducible_origin()
      throws IllegalSampleRateException {
    // 1 month ~= 4.5 weeks
    // 1/(4.5*7*24*60*60) ~= 3.6743092298647854203409758965314520870076425631981187536743 x 10^-7
    //7 or 8 places off pure math
    Assert.assertEquals(2721600000000L, sampleRateToInterval(3.67431e-7));
  }

  @Test
  public void testSampleRateToInterval_one_sample_per_week_irreducible_origin()
      throws IllegalSampleRateException {
    //1.6534391534391534391534391534391534391534391534391534391534 x 10^-6
    //6,7, or 8 off of pure math
    Assert.assertEquals(604800000000L, sampleRateToInterval(1.65344e-6));
  }

  @Test
  public void testSampleRateToInterval_one_sample_per_day_irreducible_origin()
      throws IllegalSampleRateException {
    Assert.assertEquals(86400000000L, sampleRateToInterval(0.0000115741));
  }

  @Test
  public void testSampleRateToInterval_one_sample_per_hour_irreducible_origin()
      throws IllegalSampleRateException {
    //4 to 8 off of pure math
    Assert.assertEquals(3600000000L, sampleRateToInterval(0.000277778));
  }

  @Test

  public void testSampleRateToInterval_one_sample_per_minute_irreducible_origin()
      throws IllegalSampleRateException {
    //3 to 7 off of pure math
    Assert.assertEquals(60000000L, sampleRateToInterval(0.0166667));
  }

  @Test
  public void testSampleRateToInterval_greater_than_one_irreducible()
      throws IllegalSampleRateException {
    // 11111111/60 -> 185185.18333333333 sample rate
    // True interval is 5.40000005400000054000000540000005400000054... microseconds
    Assert.assertEquals(5, sampleRateToInterval(185185));
  }

  @Test
  public void testSampleRateToInterval_one_sample_per_microsecond()
      throws IllegalSampleRateException {
    // If this ever goes below 1 sample per microsecond, space time will fold in upon itself.
    Assert.assertEquals(1, sampleRateToInterval(1000000));
  }

  @Test(expected = IllegalSampleRateException.class)
  public void testSampleRateToInterval_less_than_one_sample_per_microsecond()
      throws IllegalSampleRateException {
    // If this ever goes below 1 sample per microsecond, space time will fold in upon itself.
    sampleRateToInterval(1000001);
  }

  @Test(expected = IllegalSampleRateException.class)
  public void testSampleRateToInterval_greater_than_one_sample_per_year()
      throws IllegalSampleRateException {
    // If this ever goes below 1 sample per microsecond, space time will fold in upon itself.
    sampleRateToInterval(3.17096e-8);
  }
}