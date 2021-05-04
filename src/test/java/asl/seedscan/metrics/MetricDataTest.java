package asl.seedscan.metrics;

import asl.metadata.Channel;
import asl.metadata.Station;
import asl.metadata.meta_new.StationMeta;
import asl.seedscan.database.MetricDatabaseMock;
import asl.seedscan.database.MetricValueIdentifier;
import asl.seedsplitter.DataSet;
import asl.testutils.ResourceManager;
import asl.utils.timeseries.DataBlock;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import seed.Blockette320;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;

import static javax.xml.bind.DatatypeConverter.printHexBinary;
import static org.junit.Assert.*;

public class MetricDataTest {

  private static MetricDatabaseMock database;
  private static MetricData data;
  private static StationMeta metadata;


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    data = ResourceManager.loadANMOMainTestCase();
    Station metaStation = new Station("CU", "BCIP");
    LocalDate metaDate = LocalDate.ofYearDay(2015, 228);
    String metaLocation = "/metadata/rdseed/CU-BCIP-ascii.txt";
    metadata = ResourceManager.getMetadata(metaLocation, metaDate, metaStation);
    database = new MetricDatabaseMock();

    // BCIP - Digest and data
    LocalDate expectDate = LocalDate.parse("2015-08-16");
    Station expectStation = new Station("CU", "BCIP");
    String expectMetricName = "AvailabilityMetric";

    Channel expectChannel = new Channel("00", "LHZ");
    database.insertMockData(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        35.123456, ByteBuffer.wrap("Same".getBytes()));

    expectChannel = new Channel("00", "LH1");
    database.insertMockData(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        99.999, ByteBuffer.wrap("Same".getBytes()));

    expectChannel = new Channel("00", "LH2");
    database.insertMockData(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel), 0.00,
        ByteBuffer.wrap("Different".getBytes()));

    expectMetricName = "AnyMetric";
    expectChannel = new Channel("00", "LHZ");
    database.insertMockData(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        35.123456, ByteBuffer.wrap("Same1".getBytes()));

    expectChannel = new Channel("00", "LH1");
    database.insertMockData(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        99.999, ByteBuffer.wrap("Same1".getBytes()));

    expectChannel = new Channel("00", "LH2");
    database.insertMockData(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel), 0.00,
        ByteBuffer.wrap("Different1".getBytes()));

    // ANMO - Digest only tests
    expectMetricName = "AvailabilityMetric";
    expectDate = LocalDate.parse("2015-07-25");
    expectStation = new Station("IU", "ANMO");

    expectChannel = new Channel("10", "BH1"); // Precomputed the digest
    database.insertMockDigest(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("5965A457643242D7479373D3CCCD11C0")));
    expectChannel = new Channel("00", "LH2");
    database.insertMockDigest(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        ByteBuffer.wrap("Different".getBytes()));

    expectMetricName = "AnyMetric";
    expectChannel = new Channel("10", "BH1"); // Precomputed the digest
    database.insertMockDigest(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("5965A457643242D7479373D3CCCD11C0")));
    expectChannel = new Channel("00", "LH2");
    database.insertMockDigest(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        ByteBuffer.wrap("Different".getBytes()));
  }


  /*
   * Tests the missing data constructor.
   */
  @Test
  public final void testMetricDataMetricReaderStationMeta() throws Exception {
    MetricData metricData = new MetricData(new MetricDatabaseMock(), metadata);
    assertNotNull(metricData);
  }

  /*
   * Basic checks of a loaded metadata to ensure loading correct metadata in
   * class.
   */
  @Test
  public final void testGetMetaData_BasicInformationCorrect() throws Exception {
    StationMeta metadata = data.getMetaData();
    assertNotNull(metadata);
    assertEquals("2015:206", metadata.getDate());
    assertEquals((Double) 1820.0, (Double) metadata.getElevation());
    assertEquals("IU_ANMO", metadata.toString());
  }

  @Test
  public final void testHasChannels_Exist() throws Exception {
    // Should exist
    assertTrue(data.hasChannels("00", "LH"));
    assertTrue(data.hasChannels("10", "BH"));
    // Double Check
    assertTrue(data.hasChannels("10", "BH"));
  }

  @Test
  public final void testHasChannels_Nonexistent() throws Exception {
    // Should not exist
    assertFalse(data.hasChannels("70", "BH"));
    // 20-HNZ closed before this date
    assertFalse(data.hasChannels("20", "HN"));
    // LDO exists, but is out of the scope of this method
    assertFalse(data.hasChannels("35", "LD"));
    // Flipped parameters
    assertFalse(data.hasChannels("BH", "00"));
  }

  /*
   * Basic metric reader return value test and check if reader disconnected.
   */
  @Test
  public final void testGetMetricValue_KnownValues() throws Exception {
    MetricData metricData = new MetricData(database, metadata);

    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AvailabilityMetric";
    Channel channel = new Channel("00", "LHZ");

    double value = metricData.getMetricValue(date, metricName, station, channel);

    //Round to 7 places to match the Metric injector
    Double expected = (double) Math.round(35.123456 * 1000000d) / 1000000d;
    Double resulted = (double) Math.round(value * 1000000d) / 1000000d;
    assertEquals(expected, resulted);
  }

  @Test
  public final void testGetMetricValue_Disconnected() throws Exception {
    //Mock Data
    MetricDatabaseMock tempDatabase = new MetricDatabaseMock();
    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "RandomMetric";

    Channel channel = new Channel("00", "LHZ");
    tempDatabase.insertMockData(
        new MetricValueIdentifier(date, metricName, station, channel),
        35.123456, ByteBuffer.wrap("OtherBytes".getBytes()));
    // Check disconnected reader

    MetricData metricData = new MetricData(tempDatabase, metadata);
    //Should be there
    assertNotNull(metricData.getMetricValue(date, metricName, station, channel));

    //Disconnect
    tempDatabase.setConnected(false);
    //Now it shouldn't
    assertNull(metricData.getMetricValue(date, metricName, station, channel));
  }

  /*
   * Test if channel data is returned.
   */
  @Test
  public final void testGetChannelDataChannel() throws Exception {
    DataBlock channelData = data.getChannelData(new Channel("00", "LHZ"));

    assertNotNull(channelData);
    // Make sure we got the correct Channel back
    assertEquals("IU_ANMO_00_LHZ", channelData.getName());
  }

  @Test
  public final void testGetChannelTimingQualityDataChannel() throws Exception {
    List<Integer> timingQuality;
    timingQuality = data.getChannelTimingQualityData(new Channel("10", "BH1"));

    int numberOfQuality90Records = 0;
    int i;
    for (i = 0; i < 100; i++) {
      int quality = timingQuality.get(i);
      if (quality != 100) {
        if (quality == 90) {
          ++numberOfQuality90Records;
          if (numberOfQuality90Records >= 12) {
            fail("Encountered more 90% timing quality records than we should have!");
          }
        } else {
          fail("Timing Quality doesn't match expected -- got " + timingQuality.get(i));
        }
      }
    }

    timingQuality = data.getChannelTimingQualityData(new Channel("45", "BH1"));
    assertNull(timingQuality);

  }

  /*
   * The tests for valueDigestChanged follow this pattern:
   * Data |DigestInDB |ForceUpdate|AvailabilityMetric |Expect Null response
   * 0    |0          |0          |0                  |1
   * 0    |0          |0          |1                  |0
   * 0    |0          |1          |0                  |1
   * 0    |0          |1          |1                  |0
   * 0    |1          |0          |0                  |1
   * 0    |1          |0          |1                  |1
   * 0    |1          |1          |0                  |1
   * 0    |1          |1          |1                  |0
   */
  @Test
  public final void testValueDigestChanged_NoData_NoDigestDatabase_NoForceUpdate_NotAvailability()
      throws Exception {
    MetricData metricData = new MetricData(database, metadata);

    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "NotAvailMetric";
    Channel channel = new Channel("00", "BH1"); // Not set in reader
    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            false);
    //Nothing to compute so null
    assertNull(digest);
  }

  @Test
  public final void testValueDigestChanged_NoData_NoDigestDatabase_NoForceUpdate_Availability()
      throws Exception {
    MetricData metricData = new MetricData(database, metadata);

    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AvailabilityMetric";
    Channel channel = new Channel("00", "BH1"); // Not set in reader
    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            false);
    //Availability computes regardless of No Data so not null
    assertNotNull(digest);
  }

  @Test
  public final void testValueDigestChanged_NoData_NoDigestDatabase_ForceUpdate_NotAvailability()
      throws Exception {
    MetricData metricData = new MetricData(database, metadata);

    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AnyMetric";

    Channel channel = new Channel("00", "BH1"); // Not set in reader
    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            true);
    assertNull(digest);
  }

  @Test
  public final void testValueDigestChanged_NoData_NoDigestDatabase_ForceUpdate_Availability()
      throws Exception {
    MetricData metricData = new MetricData(database, metadata);

    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AvailabilityMetric";

    Channel channel = new Channel("00", "BH1"); // Not set in reader
    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            true);
    //Availability with forced update, should compute
    assertNotNull(digest);
  }


  @Test
  public final void testValueDigestChanged_NoData_DigestDatabase_NoForceUpdate_NotAvailability()
      throws Exception {
    MetricData metricData = new MetricData(database, metadata);

    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AnyMetric";

    Channel channel = new Channel("00", "LHZ");
    ByteBuffer digest = metricData.valueDigestChanged(channel,
        new MetricValueIdentifier(date, metricName, station, channel), false);
    //No data and not availability don't care if already computed, don't recompute.
    assertNull(digest);
  }

  @Test
  public final void testValueDigestChanged_NoData_DigestDatabase_NoForceUpdate_Availability()
      throws Exception {
    MetricData metricData = new MetricData(database, metadata);

    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AvailabilityMetric";

    Channel channel = new Channel("00", "LHZ");
    ByteBuffer digest = metricData.valueDigestChanged(channel,
        new MetricValueIdentifier(date, metricName, station, channel), false);
    //Should be null, since database has availability, it might be temporary data.
    assertNull(digest);
  }

  @Test
  public final void testValueDigestChanged_NoData_DigestDatabase_ForceUpdate_NotAvailability()
      throws Exception {
    MetricData metricData = new MetricData(database, metadata);

    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AnyMetric";

    Channel channel = new Channel("00", "LHZ");
    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            true);
    //No data, can't compute
    assertNull(digest);
  }

  @Test
  public final void testValueDigestChanged_NoData_DigestDatabase_ForceUpdate_Availability()
      throws Exception {
    MetricData metricData = new MetricData(database, metadata);

    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AvailabilityMetric";

    Channel channel = new Channel("00", "LHZ");
    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            true);
    //Force Update overrides digest in database should recompute.
    assertNotNull(digest);
  }

  /*
   * Disconnected Database test without data.
   */
  @Test
  public final void testValueDigestChanged_NoData_DatabaseDisconnected_NoForceUpdate_NotAvailability()
      throws Exception {
    //Mock Data
    MetricDatabaseMock tempDatabase = new MetricDatabaseMock();
    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    /*Metric name should match exact this should not count as AvailabilityMetric*/
    String metricName = "NotAvailabilityMetric";

    Channel channel = new Channel("00", "LHZ");
    tempDatabase.insertMockData(
        new MetricValueIdentifier(date, metricName, station, channel),
        35.123456, ByteBuffer.wrap("OtherBytes".getBytes()));
    MetricData metricData = new MetricData(tempDatabase, metadata);

    tempDatabase.setConnected(false);

    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            false);
    //No data, not Availability, can't compute
    assertNull(digest);
  }

  @Test
  public final void testValueDigestChanged_NoData_DatabaseDisconnected_NoForceUpdate_Availability()
      throws Exception {
    //Mock Data
    MetricDatabaseMock tempDatabase = new MetricDatabaseMock();
    LocalDate date = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AvailabilityMetric";

    Channel channel = new Channel("00", "LHZ");
    tempDatabase.insertMockData(
        new MetricValueIdentifier(date, metricName, station, channel),
        35.123456, ByteBuffer.wrap("OtherBytes".getBytes()));
    MetricData metricData = new MetricData(tempDatabase, metadata);
    // No data, digest in Reader, reader disconnected Non Availability Metric
    tempDatabase.setConnected(false);
    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            false);
    //This should exist because it is availability
    assertNotNull(digest);
  }


  @Test
  public final void testValueDigestChanged_Data_NoDigestDatabase_NoForceUpdate() throws Exception {
    MetricData metricData = ResourceManager.loadANMOMainTestCase();
    metricData.setMetricReader(database);
    LocalDate date = LocalDate.parse("2015-08-16");

    Station station = new Station("IU", "ANMO");
    String metricName = "AnyMetric";
    Channel channel = new Channel("10", "BH2");
    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            false);
    assertNotNull(digest);
  }

  @Test
  public final void testValueDigestChanged_Data_MatchDigestDatabase_NoForceUpdate() {
    MetricData metricData = ResourceManager.loadANMOMainTestCase();
    metricData.setMetricReader(database);
    LocalDate date = LocalDate.parse("2015-07-25");

    Station station = new Station("IU", "ANMO");
    String metricName = "AnyMetric";
    Channel channel = new Channel("10", "BH1");
    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            false);
    //Digest match don't recompute
    assertNull(digest);
  }

  @Test
  public final void testValueDigestChanged_Data_MatchDigestDatabase_ForceUpdate() throws Exception {
    MetricData metricData = ResourceManager.loadANMOMainTestCase();
    metricData.setMetricReader(database);
    LocalDate date = LocalDate.parse("2015-07-25");

    Station station = new Station("IU", "ANMO");
    String metricName = "AnyMetric";
    Channel channel = new Channel("10", "BH1");

    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            true);
    //Force update, so recompute
    assertNotNull(digest);
  }

  @Test
  public final void testValueDigestChanged_Data_MismatchDigestDatabase_NoForceUpdate()
      throws Exception {
    MetricData metricData = ResourceManager.loadANMOMainTestCase();
    metricData.setMetricReader(database);
    LocalDate date = LocalDate.parse("2015-08-16");

    Station station = new Station("IU", "ANMO");
    String metricName = "AnyMetric";

    Channel channel = new Channel("00", "LH2");
    ByteBuffer digest = metricData
        .valueDigestChanged(channel, new MetricValueIdentifier(date, metricName, station, channel),
            false);
    //Digest Mismatch recompute
    assertNotNull(digest);
  }
}
