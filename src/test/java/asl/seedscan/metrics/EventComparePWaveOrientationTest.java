package asl.seedscan.metrics;

import static org.junit.Assert.*;

import asl.seedscan.event.EventLoader;
import asl.testutils.ResourceManager;
import java.time.LocalDate;
import java.util.HashMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class EventComparePWaveOrientationTest {


  private EventComparePWaveOrientation metric;
  private static MetricData data;
  private static EventLoader eventLoader;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    try {
      data = (MetricData) ResourceManager
          .loadCompressedObject("/data/IU.NWAO.2015.299.MetricData.ser.gz", false);
      eventLoader = new EventLoader(ResourceManager.getDirectoryPath("/events"));
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
    metric = new EventComparePWaveOrientation();
    metric.setData(data);
    LocalDate date = LocalDate.of(2015, 10, 26);
    metric.setEventTable(eventLoader.getDayEvents(date));
    HashMap<String, Double> expect = new HashMap<>();
    expect.put("00-20,LHZ-LNZ",
        2.884394926482693);
    expect.put("00-20,LHND-LNND", 0.6842270334452618);
    expect.put("00-20,LHED-LNED", 1.0140182353130993);
    expect.put("10-20,LHZ-LNZ", 4.0);
    expect.put("10-20,LHND-LNND", 4.0);
    expect.put("10-20,LHED-LNED", -4.0);
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testGetVersion() throws Exception {
    metric = new EventComparePWaveOrientation();
    assertEquals(1, metric.getVersion());
  }

  @Test
  public final void testGetName() throws Exception {
    metric = new EventComparePWaveOrientation();
    assertEquals("EventComparePWaveOrientation", metric.getName());
  }

  @Test
  public final void testGetAngleToEventDefault() {
    double evtLat = 1;
    double evtLon = 5;
    double staLat = 20;
    double staLon = 35;
    
    double ang = EventComparePWaveOrientation.getAngleToEvent(evtLat, evtLon, staLat, staLon);
    assertEquals(ang, 34.8, 1.0);
  }
  
  
  
}
