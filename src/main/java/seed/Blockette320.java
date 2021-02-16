/*
 * Copyright 2013, United States Geological Survey or
 * third-party contributors as indicated by the @author tags.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/  >.
 *
 */

/*
 * Blockette320.java
 * [320]Pseudo-random Calibration Blockette (64 bytes):
 * 1  UWORD - blockette type == 320
 * 2  UWORD - offset of next blockette within record (offset to next blockette)
 * 3  BTIME - beginning of Calibration Time
 * 4  UBYTE - Reserved: do not use.
 * 5  UBYTE - Calibration Flags:
 *            [Bit 2] - If set: calibration was automatic; otherwise: manual
 *            [Bit 3] - If set: calibration continued from previous record(s)
 *            [Bit 4] - If set: random amplitudes
 * 6  ULONG - Number of .0001 second ticks for the duration of calibration
 * 7  FLOAT - Peak-to-Peak amplitude of steps in units (see Channel Identifier Blockette[52], field 9)
 * 8  CHAR*3 - Channel containing calibration input (blank if none)
 * 9  UBYTE - Reserved; do not use.
 * 10 ULONG - Reference amplitude
 * 11 CHAR*12 - Coupling of calibration signal (e.g., "Resistive" or "Capacitive")
 * 12 CHAR*12 - Rolloff characteristics        (e.g., "3dB@10Hz")
 * 13 CHAR*8  - Noise characteristics          (e.g., "White" or "Red")
 *
 */

package seed;

import asl.util.Time;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * This class represents the Blockette 320 from the SEED standard V2.4
 *
 * @author Mike Hagerty mhagerty@bc.edu
 */
public class Blockette320 extends Blockette implements Serializable {

  /**
   * Serial Version UID
   */
  private static final long serialVersionUID = 2L;
  private String calInputChannel = null;
  private long calDuration = 0L;
  private double calPeakAmp = 0.;
  private double calRefAmp = 0.;
  private LocalDateTime timestamp = null;
  private String calNoiseType = null;
  private String calCoupling = null;
  private String calFilter = null;
  private byte calFlags;

  @Override
  public short blocketteNumber() {
    return 320;
  }

  /**
   * Creates a new instance of Blockette320
   *
   * @param b - The raw bytes from an existing blockette 320
   */
  public Blockette320(byte[] b) {
    super(b);
    scanCalibrationBlockette();
  }

  private void scanCalibrationBlockette() {
    // int type = (int)bb.getShort(); // =320
    // int next = (int)bb.getShort();
    bb.position(4); // Advance forward 4 bytes in buffer

    // Scan blockette BTIME (10-Bytes) into java vars
    int year = bb.getShort(); // e.g., 1987
    int dayOfYear = bb.getShort(); // e.g., 1 = Jan. 1
    int hour = bb.get(); // Hour of day (0-23)
    // int min = bb.get() & 0x000000ff; // Min of hour(0-59)
    int min = bb.get(); // Min of hour(0-59)
    int sec = bb.get(); // Sec of min (0-59, 60 for leap seconds)
    @SuppressWarnings("unused")
    int unused = bb.get(); // Unused for data (required for alignment)
    // int seconds = (int)bb.getShort(); // .0001 seconds (0--9999)
    // int seconds = bb.getShort() & 0x0000ffff; // .0001 seconds (0--9999)
    int partialSeconds = bb.getChar(); // .0001 seconds (0--9999)

    @SuppressWarnings("unused")
    byte reserved = bb.get();
    byte calFlags = bb.get();

    LocalDate localDate = LocalDate.ofYearDay(year, dayOfYear);
    LocalTime localTime = LocalTime.of(hour, min, sec, partialSeconds * 10000);
    this.timestamp = LocalDateTime.of(localDate, localTime);

		/*
		  byte calFlags = 1<<4; if (calFlags & 0x10) --> bit 4 set = calFlags =
		  16 [Random Cal] if (calFlags & 0x08) --> bit 3 set = calFlags = 08
		  [Continuation] if (calFlags & 0x04) --> bit 2 set = calFlags = 04
		  [Automatic Cal] for (int ibit=0; ibit<8; ibit++){ //if ((calFlags &
		  0x01<<ibit) == calFlags) { if ((calFlags & 1<<ibit) == calFlags) { }
		  else { } }
		 */
    // Save duration in millisecs
    // int duration = bb.getInt()/10000; // number of .0001 second ticks for
    // the duration of calibration
    int duration = bb.getInt() / 10; // number of .0001 second ticks for the
    // duration of calibration
    float peakAmp = bb.getFloat(); // Peak-to-peak amplitude of steps
    // Get Channel with Calibration Input
    char[] charBuf = new char[3];
    for (int i = 0; i < 3; i++) {
      charBuf[i] = (char) bb.get();
    }
    String channel = new String(charBuf);
    bb.get();
    // Get Reference Amplitude
    int refAmp = bb.getInt(); // Ref Amp is an unsigned long, 32-bit
    // Get Coupling, Rolloff and Noise type (all = ASCII strings)
    charBuf = new char[12];
    for (int i = 0; i < 12; i++) {
      charBuf[i] = (char) bb.get();
    }
    String coupling = new String(charBuf);

    charBuf = new char[12];
    for (int i = 0; i < 12; i++) {
      charBuf[i] = (char) bb.get();
			/*
			  char c = (char)bb.get(); if (Character.isLetterOrDigit(c)) {
			  charBuf[i] = (char) bb.get(); } else { charBuf[i] = ' '; }
			 */
    }
    String rolloff = new String(charBuf);

    charBuf = new char[8];
    for (int i = 0; i < 8; i++) {
      charBuf[i] = (char) bb.get();
    }
    String noiseType = new String(charBuf);

    this.calInputChannel = channel;
    this.calDuration = duration;
    this.calPeakAmp = peakAmp;
    this.calRefAmp = (float) refAmp;
    this.calNoiseType = noiseType;
    this.calCoupling = coupling;
    this.calFilter = rolloff;
    this.calFlags = calFlags;

  }

  public String toString() {

    return "\n== Random Calibration Blockette\n" +
        String.format("==   Start Time:%s\n", timestamp.toString()) +
        String.format("==   Calibration Duration: %d\n",
            calDuration / 1000) + // Convert millisecs --> secs for printing
        String.format(
            "==   Noise Type [%s]  Calibration Amplitude:%f\n",
            calNoiseType, calPeakAmp) +
        String.format("==   Calibration Input Channel:%s\n",
            calInputChannel) +
        String.format("==   Reference Amplitude:%f\n", calRefAmp) +
        String.format("==   Coupling Method:%s\n", calCoupling) +
        String.format(
            "==   Filtering Type:%s          Calibration Flags:[%02x]\n",
            calFilter, calFlags) +
        "====================================";
  }

  public long getCalibrationEpoch() {
    return Time.calculateEpochMilliSeconds(timestamp);
  }

  public LocalDateTime getCalibrationTimeStamp() {
    return timestamp;
  }

  public long getCalibrationDuration() {
    return calDuration;
  }

  public String getCalInputChannel() {
    return calInputChannel;
  }

  public double getCalPeakAmp() {
    return calPeakAmp;
  }

}
