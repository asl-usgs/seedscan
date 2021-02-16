package asl.seedscan;

import asl.seedscan.config.ConfigT;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * The Class ConfigParser.
 * <p>
 * Reads the XSD schema used by JAXB and confirms that config.xml is properly formatted.
 */
class ConfigParser {

  /**
   * The Constant logger.
   */
  private static final Logger logger = LoggerFactory.getLogger(asl.seedscan.ConfigParser.class);

  /**
   * The schema.
   */
  private Schema schema = null;

  /**
   * Instantiates a new config parser.
   *
   * @param schemaFiles the schema files
   */
  ConfigParser(Collection<URL> schemaFiles) {
    schema = makeSchema(schemaFiles);
  }

  /**
   * Make schema from a list of file URLs.
   *
   * @param files the files
   * @return the schema
   */
  private Schema makeSchema(Collection<URL> files) {
    Schema schema = null;
    StreamSource[] sources = new StreamSource[files.size()];

    int i = 0;
    for (URL fileURL : files) {
      try {
        sources[i] = new StreamSource(fileURL.openStream());

      } catch (IOException e) {
        logger.error("Unable to read file: {}", e.getMessage());
      }
      i++;
    }

    SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    try {
      schema = factory.newSchema(sources);
    } catch (SAXException ex) {
      String message = "SAXException: Could not generate schema from supplied files:";
      logger.error(message, ex);
    }

    return schema;
  }

  /**
   * Parse configuration file
   *
   * @param configFile configuration File
   * @return parse ConfigT
   * @throws FileNotFoundException if the file is not found or readable
   * @throws JAXBException         if the configuration cannot be marshalled
   */
  ConfigT parseConfig(File configFile) throws FileNotFoundException, JAXBException {
    ConfigT cfg = null;

    JAXBContext context = JAXBContext.newInstance("asl.seedscan.config");
    Unmarshaller unmarshaller = context.createUnmarshaller();
    unmarshaller.setSchema(schema);
    InputStream stream = new BufferedInputStream(
        new DataInputStream(new FileInputStream(configFile)));
    StreamSource source = new StreamSource(stream);
    JAXBElement<ConfigT> cfgRoot = unmarshaller.unmarshal(source, ConfigT.class);
    cfg = cfgRoot.getValue();

    return cfg;
  }

}
