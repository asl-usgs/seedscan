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
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("699EAAE66CF4C20716D7C695A9DE0DD9")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","BH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C79DE3622B2000130E790A5446101E01")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","BHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("2731D7A25401CD54836EF851201D202C")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","LH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("D9DF0B9A41E65F46E81A1FF1CB3E90C0")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","LH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("8CD6822C6CBE39245D142C67AD6A283D")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","LHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("238059BFC152D1AA9BD650512506D20C")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("BBB8C07A767208F123CB8D59DFBB2329")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("F240CEC139409DB381F59C9EBB5B42EF")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("48764E568FEC9963453965348B208F1B")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","BH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("241DEEADAF1CA2FD4A654D483029F1F3")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","BH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("8C1902E5D159FC597DAB15CEBEBFEBD4")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","BHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("D6230C7AA35F7E34298CDA935E648A56")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","LH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("3221360DB5D42818DE5609FD0C84CE73")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","LH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("4260A7277B760B8C56149F363266696E")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","LHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("33FED87A726469CA192587F6D4B9B969")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VH1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("CA706A92F06E4DA1F1D66FCB972AB7D0")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VH2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("4CA44B8C180B95ACEA3C70AA54230575")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VHZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("315D9D1E66742BCCBA3429213695C810")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20","LN1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("1FDA3928D007A720B19E6FDAFB527AF7")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20","LN2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("C7D4393D27AA21A5DAA19B0BC1D17101")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("20","LNZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("D27C4946D7F08F398FD3E66BA1798C19")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("31","LDO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("255708262E4FB1EF0FCB14DAD56A369E")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("35","LDO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("E9CA1FBA2A990D1035A5CB332D063C18")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LDO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("22B1689D0485E92991259C85EFD73A13")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LIO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("35DDB60900C489DBE0A793A3513A30EB")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LKO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("4B99938340F37401114732AC20529E07")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LRH")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("F9A58BE71816472AE237B583E4AC7419")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LRI")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("F9A58BE71816472AE237B583E4AC7419")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LWD")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("165C8D48BEBFDA64E6837C276035A4CD")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("50","LWS")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("D5DC83C619B19205249A7F66C5CA7DB6")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("60","HDF")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("8107BEE8208A3C8671166F4E76369A7C")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VM1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("5CC3259EB1D31B7775C1FA6270820A00")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VM2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("22FE293A395EF3A98167DFBCD77DA3EA")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("00","VMZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("D26C89E8FA1BCFFB182C1FED512C4051")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VMU")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("23137842457ED5C9AD8A927D62B37F5A")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VMV")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("23137842457ED5C9AD8A927D62B37F5A")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("10","VMW")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("23137842457ED5C9AD8A927D62B37F5A")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("30","LDO")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("6BE526346BB465B0A16DFC05968852E4")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("40","LF1")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("53D3891CB62BA202D2DAF0B3B7F36961")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("40","LF2")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("85879B16B7066492C9CAEC37286BB1C0")));
    database.insertMockData(
        new MetricValueIdentifier(expectDate, metricName, station, new Channel("40","LFZ")),
        100.0, ByteBuffer.wrap(DatatypeConverter.parseHexBinary("AFB970A4486FE4FC902AEDF5D60B967A")));

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
