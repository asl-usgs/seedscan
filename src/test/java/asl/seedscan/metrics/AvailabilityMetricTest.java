package asl.seedscan.metrics;

import static org.junit.Assert.assertEquals;

import asl.metadata.Channel;
import asl.metadata.ChannelArray;
import asl.metadata.Station;
import asl.metadata.meta_new.StationMeta;
import asl.seedscan.database.MetricDatabaseMock;
import asl.seedscan.database.MetricValueIdentifier;
import asl.testutils.ResourceManager;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.HashMap;
import javax.xml.bind.DatatypeConverter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AvailabilityMetricTest {

  private static MetricData maleableData;
  private static MetricData data;
  private static StationMeta metadata;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    data = ResourceManager.loadANMOMainTestCase();
    maleableData = ResourceManager.loadANMOMainTestCase();

    Station metaStation = new Station("CU", "BCIP");
    LocalDate metaDate = LocalDate.ofYearDay(2015, 228);
    String metaLocation = "/metadata/rdseed/CU-BCIP-ascii.txt";
    metadata = ResourceManager.getMetadata(metaLocation, metaDate, metaStation);

    MetricDatabaseMock mockdatabase = new MetricDatabaseMock();

    String expectMetricName = "AvailabilityMetric";
    LocalDate expectDate = LocalDate.parse("2015-07-25");
    Station expectStation = new Station("IU", "ANMO");

    Channel expectChannel = new Channel("10", "BH1"); // Precomputed the digest
    mockdatabase.insertMockDigest(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("9A4FE3A10FD60F93526F464B0DB9580E")));
    expectChannel = new Channel("00", "LH2");
    mockdatabase.insertMockDigest(
        new MetricValueIdentifier(expectDate, expectMetricName, expectStation, expectChannel),
        ByteBuffer.wrap("Different".getBytes()));

    maleableData.setMetricReader(mockdatabase);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    maleableData = null;
    data = null;
  }

  @Test
  public final void testGetName() throws Exception {
    Metric metric = new AvailabilityMetric();
    assertEquals("AvailabilityMetric", metric.getName());
  }

  @Test
  public final void testGetVersion() throws Exception {
    Metric metric = new AvailabilityMetric();
    assertEquals(1, metric.getVersion());
  }

  @Test
  public final void testProcess_HasData_NoDB() throws Exception {
    Metric metric = new AvailabilityMetric();
    metric.setData(data);

    /* Should be no HN data in here as it is Triggered*/
    HashMap<String, Double> expect = new HashMap<>();
    expect.put("00,BH1", 100.0);
    expect.put("00,BH2", 100.0);
    expect.put("00,BHZ", 100.0);
    expect.put("00,LH1", 100.0);
    expect.put("00,LH2", 100.0);
    expect.put("00,LHZ", 100.0);
    expect.put("00,VH1", 100.0);
    expect.put("00,VH2", 100.0);
    expect.put("00,VHZ", 100.0);
    expect.put("00,VM1", 100.0);
    expect.put("00,VM2", 100.0);
    expect.put("00,VMZ", 100.0);
    expect.put("10,BH1", 100.0);
    expect.put("10,BH2", 100.0);
    expect.put("10,BHZ", 100.0);
    expect.put("10,LH1", 100.0);
    expect.put("10,LH2", 100.0);
    expect.put("10,LHZ", 100.0);
    expect.put("10,VH1", 100.0);
    expect.put("10,VH2", 100.0);
    expect.put("10,VHZ", 100.0);
    expect.put("10,VMU", 100.0);
    expect.put("10,VMV", 100.0);
    expect.put("10,VMW", 100.0);
    expect.put("20,LN1", 100.0);
    expect.put("20,LN2", 100.0);
    expect.put("20,LNZ", 100.0);
    expect.put("30,LDO", 100.0);
    expect.put("31,LDO", 100.0);
    expect.put("35,LDO", 100.0);
    expect.put("40,LF1", 100.0);
    expect.put("40,LF2", 100.0);
    expect.put("40,LFZ", 100.0);
    expect.put("50,LDO", 100.0);
    expect.put("50,LIO", 100.0);
    expect.put("50,LKO", 100.0);
    expect.put("50,LRH", 100.0);
    expect.put("50,LRI", 100.0);
    expect.put("50,LWD", 100.0);
    expect.put("50,LWS", 100.0);
    expect.put("60,HDF", 100.0);
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testProcess_HasData_HasDB() throws Exception {
    MetricDatabaseMock database = new MetricDatabaseMock();

    String metricName = "AvailabilityMetric";
    LocalDate expectDate = LocalDate.parse("2015-07-25");
    Station station = new Station("IU", "ANMO");
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","BH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("4D46DFED1F5EF9DF5EE29021AB471FA7")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","BH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("64BC1D72F04D8610BF7A9240860C7C8A")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","BHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("4B2C9C6A11E8ABB0CB6287837B157CEF")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","LH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("D61F8DB8FFF1DF3FFEB9D089C9514EA9")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","LH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("36B4BB961B6CB41CD6BBD460CD524DF3")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","LHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("818630B82BB59C60E3765C4BDF1E346B")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("AEA92846AE204421135BAC048640375A")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("4B327253DBF39CA3CD61498F6C8F554B")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("B0C996A60DE923D098D8C230BC6D35AC")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","BH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("5965A457643242D7479373D3CCCD11C0")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","BH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("F161481F1A771C73705D2B55425B0BE7")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","BHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("AB5B468068719E1E247AE408A2806A65")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","LH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("1AF48BC0019BE0A18A4CAA0090A5A814")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","LH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6AB51AEACF39C335020E3CCBAE470F09")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","LHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("1B2B65B7C62BA1734D3F240B4898DF0E")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("FE549DEC30A74844F0BF4997B8CF4030")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("5FE160E0E605631BF2DE2E5739BE55E6")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("05796A60A6BD2E29BB5D0F7D19703FF0")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20","LN1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("D7ACA4459F4614EDFF8B8DB02E5ED8B0")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20","LN2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("0FA2A45068EB926894B4796114DDD346")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20","LNZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("1A0AD42B98B13CF4C1C6040174752E5E")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("31","LDO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("605F5FBB2888DA43AE5F2AFB995A4FF3")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("35","LDO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("B3125AB38F251E6E6FDFEEB0C61062BB")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LDO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("51A283D87D715FF80ACFC96E1A211C53")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LIO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("A03DFCA30497FC7131E2BE72C8772C73")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LKO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("DE79D92944A001ABC0021F7DB91F829F")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LRH")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6C45C14D1C453280337298527DE16881")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LRI")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6C45C14D1C453280337298527DE16881")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LWD")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("4DA677ED57FA26F28DD681F1D0F1D8FB")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LWS")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("403CC96C1DE2E7AFF5DF52B75C87612E")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("60","HDF")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("38F4E703C7F60C00051EE217121CD266")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VM1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("FD081C437207AA2333807C0811F5062A")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VM2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("DECC9444654224519F4E88EBF35F0455")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VMZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C9D42B09FBD80E15B90E45593DBB8C1C")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VMU")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("B35DCD9467666201B4DAB53D05E60BF3")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VMV")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("B35DCD9467666201B4DAB53D05E60BF3")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VMW")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("B35DCD9467666201B4DAB53D05E60BF3")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("30","LDO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("313D633DCE0876CEFB17D9867D9E0C47")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("40","LF1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("9490F8C830DA9A7F43F0CFD5E668F833")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("40","LF2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("42C4EAC231F75CEF58E0D35179F02092")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("40","LFZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("68FA0170CE9EDC810100D29387900728")));

    Metric metric = new AvailabilityMetric();
    MetricData metricData = ResourceManager.loadANMOMainTestCase();
    metricData.setMetricReader(database);
    metric.setData(metricData);

    HashMap<String, Double> expect = new HashMap<>();
    //All Digests should match so no new results.
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testProcess_NoData_NoDB() throws Exception {
    Metric metric = new AvailabilityMetric();
    MetricData metricData = new MetricData(new MetricDatabaseMock(), metadata);
    metric.setData(metricData);
    HashMap<String, Double> expect = new HashMap<>();
    expect.put("20,LNZ", 0.0);
    expect.put("00,VMW", 0.0);
    expect.put("00,BH2", 0.0);
    expect.put("00,VMV", 0.0);
    expect.put("00,BH1", 0.0);
    expect.put("00,VMU", 0.0);
    expect.put("00,LH2", 0.0);
    expect.put("00,LH1", 0.0);
    expect.put("00,BHZ", 0.0);
    expect.put("00,LHZ", 0.0);
    expect.put("20,LN2", 0.0);
    expect.put("20,LN1", 0.0);
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testProcess_NoData_HasDB() throws Exception {
    MetricDatabaseMock database = new MetricDatabaseMock();
    LocalDate expectDate = LocalDate.parse("2015-08-16");
    Station station = new Station("CU", "BCIP");
    String metricName = "AvailabilityMetric";
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20", "LNZ")),
        100.0,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6E04CC6659D82C1DC784A36371BF0335")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "VMW")),
        99.9,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C8280EC927F64731B993EFA825D52929")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "BH2")),
        98.0,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("828D4BDF7B8194E4F721B6D701C6ED44")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "VMV")),
        40.5,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C8280EC927F64731B993EFA825D52929")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "BH1")),
        35.5,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("828D4BDF7B8194E4F721B6D701C6ED44")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "VMU")),
        10.23,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C8280EC927F64731B993EFA825D52929")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "LH2")),
        0.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("346999B1DBB719CCD2E117DC95832B83")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "LH1")),
        78.9,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("346999B1DBB719CCD2E117DC95832B83")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "BHZ")),
        5.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("828D4BDF7B8194E4F721B6D701C6ED44")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00", "LHZ")),
        10.0,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("346999B1DBB719CCD2E117DC95832B83")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20", "LN2")),
        100.0,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6E04CC6659D82C1DC784A36371BF0335")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20", "LN1")),
        100.0,
        ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6E04CC6659D82C1DC784A36371BF0335")));

    Metric metric = new AvailabilityMetric();
    MetricData metricData = new MetricData(database, metadata);
    metric.setData(metricData);

    HashMap<String, Double> expect = new HashMap<>();
    /* Should have no new results. Availability doesn't replace existing values if it has no data.*/
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testProcess_NoDerivedChannels_NoTriggered() throws Exception {
    Metric metric = new AvailabilityMetric();
    MetricData metricData = ResourceManager.loadANMOMainTestCase();

    //Rotate and add Derived channels to metricData
    metricData.checkForRotatedChannels(new ChannelArray("00", "LHND", "LHED", "LHZ"));
    metric.setData(metricData);

    /* Should be no Triggered HN or HH channels*/
    /* Should be no Derived channels*/
    HashMap<String, Double> expect = new HashMap<>();
    expect.put("00,BH1", 100.0);
    expect.put("00,BH2", 100.0);
    expect.put("00,BHZ", 100.0);
    expect.put("00,LH1", 100.0);
    expect.put("00,LH2", 100.0);
    expect.put("00,LHZ", 100.0);
    expect.put("00,VH1", 100.0);
    expect.put("00,VH2", 100.0);
    expect.put("00,VHZ", 100.0);
    expect.put("00,VM1", 100.0);
    expect.put("00,VM2", 100.0);
    expect.put("00,VMZ", 100.0);
    expect.put("10,BH1", 100.0);
    expect.put("10,BH2", 100.0);
    expect.put("10,BHZ", 100.0);
    expect.put("10,LH1", 100.0);
    expect.put("10,LH2", 100.0);
    expect.put("10,LHZ", 100.0);
    expect.put("10,VH1", 100.0);
    expect.put("10,VH2", 100.0);
    expect.put("10,VHZ", 100.0);
    expect.put("10,VMU", 100.0);
    expect.put("10,VMV", 100.0);
    expect.put("10,VMW", 100.0);
    expect.put("20,LN1", 100.0);
    expect.put("20,LN2", 100.0);
    expect.put("20,LNZ", 100.0);
    expect.put("30,LDO", 100.0);
    expect.put("31,LDO", 100.0);
    expect.put("35,LDO", 100.0);
    expect.put("40,LF1", 100.0);
    expect.put("40,LF2", 100.0);
    expect.put("40,LFZ", 100.0);
    expect.put("50,LDO", 100.0);
    expect.put("50,LIO", 100.0);
    expect.put("50,LKO", 100.0);
    expect.put("50,LRH", 100.0);
    expect.put("50,LRI", 100.0);
    expect.put("50,LWD", 100.0);
    expect.put("50,LWS", 100.0);
    expect.put("60,HDF", 100.0);
    TestUtils.testMetric(metric, expect);
  }

  @Test
  public final void testProcess_ASA1_failing_to_compute_nonzero_value() throws Exception {
    Metric metric = new AvailabilityMetric();
    MetricData metricData = ResourceManager.loadASA1MainTestCase();
    metric.setData(metricData);
    HashMap<String, Double> expect = new HashMap<>();
    expect.put("00,HH1", 100.0);
    expect.put("00,HH2", 100.0);
    expect.put("00,HHZ", 100.0);
    expect.put("00,LH1", 100.0);
    expect.put("00,LH2", 100.0);
    expect.put("00,LHZ", 100.0);
    expect.put("00,VM1", 100.0);
    expect.put("00,VM2", 100.0);
    expect.put("00,VM3", 100.0);
    TestUtils.testMetric(metric, expect);
  }
}
