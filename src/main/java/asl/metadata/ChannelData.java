package asl.metadata;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelData {
	private static final Logger logger = LoggerFactory
			.getLogger(asl.metadata.ChannelData.class);

	private static final int CHANNEL_EPOCH_INFO_BLOCKETTE_NUMBER = 52;
	private static final int CHANNEL_COMMENT_BLOCKETTE_NUMBER = 59;

  private final Hashtable<LocalDateTime, EpochData> epochs;
	private final String location;
	private final String name;

	ChannelData(ChannelKey channelKey) {
		this.location = channelKey.getLocation();
		this.name = channelKey.getName();
    Hashtable<LocalDateTime, Blockette> comments = new Hashtable<>();
		epochs = new Hashtable<>();
	}

	// identifiers
	public String getLocation() {
		return location;
	}

	public String getName() {
		return name;
	}

	// epochs
	LocalDateTime addEpoch(Blockette blockette)
			throws MissingBlocketteDataException, TimestampFormatException,
			WrongBlocketteException {
		if (blockette.getNumber() != CHANNEL_EPOCH_INFO_BLOCKETTE_NUMBER) {
			throw new WrongBlocketteException();
		}
		// Epoch epochNew = new Epoch(blockette);
		String timestampString = blockette.getFieldValue(22, 0);

		if (timestampString == null) {
			throw new MissingBlocketteDataException();
		}
		LocalDateTime timestamp = BlocketteTimestamp
                .parseTimestamp(timestampString);
		EpochData data = new EpochData(blockette);
		epochs.put(timestamp, data);
		return timestamp;
	}

	public boolean hasEpoch(LocalDateTime timestamp) {
		return epochs.containsKey(timestamp);
	}

	EpochData getEpoch(LocalDateTime timestamp) {
		return epochs.get(timestamp);
	}

	// containsEpoch - search through epochs of current channeldata
	// return true if epochTime is contained.
	/**
	 * Epoch index ----------- ONLY THIS EPOCH! 0 newest startTimestamp - newest
	 * endTimestamp (may be "null") 1 ... - ... 2 ... - ... . ... - ... n-1
	 * oldest startTimestamp - oldest endTimestamp
	 **/
	// public boolean containsEpoch(Calendar epochTime)
	LocalDateTime containsEpoch(LocalDateTime epochTime) {
		boolean containsEpochTime = false;

    ArrayList<LocalDateTime> epochtimes = new ArrayList<>(epochs.keySet());
		Collections.sort(epochtimes);
		Collections.reverse(epochtimes);
		int nEpochs = epochtimes.size();

		// epochs keys(=timestamps) are now sorted with the newest first
		// most likely the first (=newest) epoch will be the one we want
		// So check the first epoch for the epochTime and if it's not
		// found (and nEpochs > 1), check the older epochs

    LocalDateTime timestamp = epochtimes.get(0);
    EpochData epoch = epochs.get(timestamp);
    LocalDateTime startTimeStamp = epoch.getStartTime();
    LocalDateTime endTimeStamp = epoch.getEndTime();
		if (endTimeStamp == null) { // This Epoch is open
			if (epochTime.compareTo(startTimeStamp) >= 0) {
				containsEpochTime = true;
			}
		} // The first Epoch is closed
		else if (epochTime.compareTo(startTimeStamp) >= 0
				&& epochTime.compareTo(endTimeStamp) <= 0) {
			containsEpochTime = true;
		}

		if (!containsEpochTime && nEpochs > 1) { // Search the older epochs if
													// necessary
			for (int i = 1; i < nEpochs; i++) {
				timestamp = epochtimes.get(i);
				epoch = epochs.get(timestamp);
				startTimeStamp = epoch.getStartTime();
				endTimeStamp = epoch.getEndTime();
				if (endTimeStamp == null) { // This Epoch is open - we don't
											// allow that here!
					logger.error("Older Epoch has Open End Time (=null)");
          break;
				} else if (epochTime.compareTo(startTimeStamp) >= 0
						&& epochTime.compareTo(endTimeStamp) <= 0) {
					containsEpochTime = true;
					break;
				}
			}
		}

		if (containsEpochTime) {
      return startTimeStamp;
		} else {
			return null;
		}

	}

	void printEpochs() {
    ArrayList<LocalDateTime> epochtimes = new ArrayList<>(epochs.keySet());
		Collections.sort(epochtimes);

		for (LocalDateTime timestamp : epochtimes) {
			// timestamp is the Hashtable key and "should" be the same as
			// EpochData.getStartTime()

			EpochData epoch = epochs.get(timestamp);
			LocalDateTime startTimeStamp = epoch.getStartTime();
			LocalDateTime endTimeStamp = epoch.getEndTime();
			String startDateString = EpochData
					.epochToDateString(startTimeStamp);
			String endDateString = EpochData.epochToDateString(endTimeStamp);
		}
	}

}
