package asl.metadata;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StationData {
	private static final Logger logger = LoggerFactory
			.getLogger(asl.metadata.StationData.class);

	private static final int STATION_EPOCH_BLOCKETTE_NUMBER = 50;

	private final Hashtable<LocalDateTime, Blockette> epochs;
	private final Hashtable<ChannelKey, ChannelData> channels;
	private final String network;
	private final String name;

	public StationData(String network, String name) {
		epochs = new Hashtable<>();
		channels = new Hashtable<>();
		this.name = name;
		this.network = network;
	}

	public String getNetwork() {
		return network;
	}

	public String getName() {
		return name;
	}

	public LocalDateTime addEpoch(Blockette blockette)
			throws TimestampFormatException, WrongBlocketteException,
			MissingBlocketteDataException {
		if (blockette.getNumber() != STATION_EPOCH_BLOCKETTE_NUMBER) {
			throw new WrongBlocketteException();
		}

		String timestampString = blockette.getFieldValue(13, 0);
		if (timestampString == null) {
			throw new MissingBlocketteDataException();
		}
		
		LocalDateTime timestamp = BlocketteTimestamp
				.parseTimestamp(timestampString);
		epochs.put(timestamp, blockette);
		return timestamp;
	}

	public boolean hasEpoch(LocalDateTime timestamp) {
		return epochs.containsKey(timestamp);
	}

	public Blockette getEpoch(LocalDateTime timestamp) {
		return epochs.get(timestamp);
	}

	/**
	 * Epoch index ----------- ONLY THIS EPOCH! 0 newest startTimestamp - newest
	 * endTimestamp (may be "null") 1 ... - ... 2 ... - ... . ... - ... n-1
	 * oldest startTimestamp - oldest endTimestamp
	 **/

	// Return the correct Blockette 050 for the requested epochTime
	// Return null if epochTime not contained
	public Blockette getBlockette(LocalDateTime epochTime) {
    ArrayList<LocalDateTime> epochtimes = new ArrayList<>(epochs.keySet());
		Collections.sort(epochtimes);
		Collections.reverse(epochtimes);

		LocalDateTime startTimeStamp;
		LocalDateTime endTimeStamp;

		// Loop through Blockettes (B050) and pick out epoch end dates
    for (LocalDateTime epochtime : epochtimes) {
      endTimeStamp = null;
      startTimeStamp = epochtime;
      Blockette blockette = epochs.get(startTimeStamp);
      String timestampString = blockette.getFieldValue(14, 0);
      if (!timestampString.equals("(null)")) {
        try {
          endTimeStamp = BlocketteTimestamp
              .parseTimestamp(timestampString);
        } catch (TimestampFormatException e) {
          logger.error("StationData.printEpochs() [{}-{}] Error converting timestampString={}",
              network, name, timestampString);
        }
      }
      if (endTimeStamp == null) { // This Epoch is open
        if (epochTime.compareTo(startTimeStamp) >= 0) {
          return blockette;
        }
      } // This Epoch is closed
      else if (epochTime.compareTo(startTimeStamp) >= 0
          && epochTime.compareTo(endTimeStamp) <= 0) {
        return blockette;
      }
    }
		return null; // If we made it to here than we are returning
						// blockette==null
	}

	public void addChannel(ChannelKey key, ChannelData data) {
		channels.put(key, data);
	}

	public boolean hasChannel(ChannelKey key) {
		return channels.containsKey(key);
	}

	public ChannelData getChannel(ChannelKey key) {
		return channels.get(key);
	}

	public Hashtable<ChannelKey, ChannelData> getChannels() {
		return channels;
	}

}
