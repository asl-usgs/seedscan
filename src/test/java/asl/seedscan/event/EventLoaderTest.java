package asl.seedscan.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import asl.metadata.Station;
import asl.testutils.ResourceManager;
import java.time.LocalDate;
import java.util.Hashtable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import edu.sc.seis.seisFile.sac.SacTimeSeries;

public class EventLoaderTest {

  private static EventLoader eventLoader;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    eventLoader = new EventLoader(ResourceManager.getDirectoryPath("/event_synthetics"));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    eventLoader = null;
  }

  @Test
  public final void testEventLoader() throws Exception {
    String correctEventDir = EventLoader.getEventsDirectory();
    // Try to change the directory, shouldn't happen.
    new EventLoader(ResourceManager.getDirectoryPath("/"));
    assertEquals(EventLoader.getEventsDirectory(), correctEventDir);
  }

  @Test
  public final void testGetDaySyntheticsOldName() {
    LocalDate date = LocalDate.of(2001, 1, 1);
    Station station = new Station("IU", "ANMO");

    eventLoader.getDayEvents(date); // TODO: This shouldn't be required.
    Hashtable<String, Hashtable<String, SacTimeSeries>> synthetics = eventLoader
        .getDaySynthetics(date, station);
    assertNotNull(synthetics);
    Hashtable<String, SacTimeSeries> eventSynthetics = synthetics.get("C010101B");

    SacTimeSeries timeseries = eventSynthetics.get("ANMO.XX.LXZ.modes.sac.proc");
    assertEquals(timeseries.getNumPtsRead(), 3999);
    timeseries = eventSynthetics.get("ANMO.XX.LXZ.modes.sac");
    assertEquals(timeseries.getNumPtsRead(),8000);
  }

  @Test
  public final void testGetDaySynthetics() {
    LocalDate date = LocalDate.of(2015, 10, 26);
    Station station = new Station("IU", "NWAO");

    eventLoader.getDayEvents(date); // TODO: This shouldn't be required.
    Hashtable<String, Hashtable<String, SacTimeSeries>> synthetics = eventLoader
        .getDaySynthetics(date, station);
    assertNotNull(synthetics);
    Hashtable<String, SacTimeSeries> eventSynthetics = synthetics.get("C201510260909A");

    SacTimeSeries timeseries = eventSynthetics.get("NWAO.XX.LXZ.modes.sac.proc");
    assertEquals(timeseries.getNumPtsRead(), 3999);
    timeseries = eventSynthetics.get("NWAO.XX.LXZ.modes.sac");
    assertEquals(timeseries.getNumPtsRead(),8000);

    // Should only return table with synthetics that are actually there.
    station = new Station("IU", "MAKZ");
    synthetics = eventLoader.getDaySynthetics(date, station);
    assertTrue(synthetics.isEmpty());

  }

  @Test
  public final void testGetDayEvents() throws Exception {
    LocalDate date = LocalDate.of(2015, 10, 26);
    Hashtable<String, EventCMT> cmts = eventLoader.getDayEvents(date);
    EventCMT cmt = cmts.get("C201510260909A");
    assertEquals(cmt.getLatitude(), 36.44, 0.);
    assertEquals(cmt.getLongitude(), 70.72, 0.);
    assertEquals(cmt.getDepth(), 212.5, 0.);
  }

}
