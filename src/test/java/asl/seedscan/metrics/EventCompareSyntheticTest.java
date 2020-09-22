package asl.seedscan.metrics;

import static org.junit.Assert.assertEquals;

import asl.metadata.Station;
import asl.seedscan.event.EventLoader;
import asl.testutils.MetricTestMap;
import asl.testutils.ResourceManager;
import java.time.LocalDate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class EventCompareSyntheticTest {

  private EventCompareSynthetic metric;
  private static MetricData data;
  private static EventLoader eventLoader;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    try {
      data = ResourceManager.loadNWAOMainTestCase();
      eventLoader = new EventLoader(ResourceManager.getDirectoryPath("/event_synthetics"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    data = null;
    eventLoader = null;
  }

  @Test
  public void testProcessDefault() throws Exception {
    metric = new EventCompareSynthetic();
    metric.setData(data);
    LocalDate date = LocalDate.of(2015, 10, 26);
    metric.setEventTable(eventLoader.getDayEvents(date));
    metric.setEventSynthetics(eventLoader.getDaySynthetics(date, new Station("IU", "NWAO")));
    MetricTestMap expect = new MetricTestMap();
    double error = 1E-4;
    expect.put("00,LHZ",   0.78276, error);  //Before hard coding removal.  0.7365784417165183
    expect.put("00,LHND",  0.87600, error); //0.8034777781560093
    expect.put("00,LHED",  0.81786, error); //0.7419806164967785
    expect.put("10,LHZ",   0.00356, error); //0.0002246588435010572
    expect.put("10,LHND", -0.00430, error); //0.00008719589964150271
    expect.put("10,LHED", -0.00023, error); //Nonexistent

    TestUtils.testMetric(metric, expect);

  }

  @Test
  public void testProcessCustom() throws Exception {
    metric = new EventCompareSynthetic();

		/*We can't test non LX synthetics, because we don't have any. Manually setting to XX-LX and LH is the closest we can get.*/
    metric.add("base-channel", "XX-LX");
    metric.add("channel-restriction", "LH");

    metric.setData(data);
    LocalDate date = LocalDate.of(2015, 10, 26);
    metric.setEventTable(eventLoader.getDayEvents(date));
    metric.setEventSynthetics(eventLoader.getDaySynthetics(date, new Station("IU", "NWAO")));
    MetricTestMap expect = new MetricTestMap();
    double error = 1E-4;
    expect.put("00,LHZ",   0.78276, error);  //Before hard coding removal.  0.7365784417165183
    expect.put("00,LHND",  0.87600, error); //0.8034777781560093
    expect.put("00,LHED",  0.81786, error); //0.7419806164967785
    expect.put("10,LHZ",   0.00356, error); //0.0002246588435010572
    expect.put("10,LHND", -0.00430, error); //0.00008719589964150271
    expect.put("10,LHED", -0.00023, error); //Nonexistent

    TestUtils.testMetric(metric, expect);

  }

  @Test
  public final void testGetVersion() throws Exception {
    metric = new EventCompareSynthetic();
    assertEquals(3, metric.getVersion());
  }

  @Test
  public final void testGetName() throws Exception {
    metric = new EventCompareSynthetic();
    assertEquals("EventCompareSynthetic", metric.getName());
  }
}
