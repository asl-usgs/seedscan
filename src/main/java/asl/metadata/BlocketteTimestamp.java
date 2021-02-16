package asl.metadata;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class BlocketteTimestamp {

  public static LocalDateTime parseTimestamp(String timestampString)
      throws TimestampFormatException {
    int year;
    int dayOfYear = 1;
    int hour = 0;
    int minute = 0;
    int second = 0;

    try {
      // MTH: this will work for something like: End date: 2012,158,23:59
      // but NOT: End date: 2012,158:23:59 - Probably should be more
      // robust (??)
      // Here's another miss:
      // ** It's corrected now date: 1995,284,00:00:00.0000

      String[] dateParts = timestampString.split(",");
      // System.out.format("== parseTimestamp(%s) Num dateParts=%d\n",
      // timestampString, dateParts.length );
      // There should be no more than three parts:
      // year,day-of-year,time
      if (dateParts.length > 3) {
        throw new TimestampFormatException();
      }
      if (dateParts.length > 0) {
        year = Integer.parseInt(dateParts[0]);
      }
      // An empty date is invalid
      else {
        throw new TimestampFormatException();
      }
      if (dateParts.length > 1) {
        dayOfYear = Integer.parseInt(dateParts[1]);
      }

      if (dateParts.length > 2) {
        String[] timeParts = dateParts[2].split(":");
        // There should be no more than three parts:
        // hour,minute,second

        // There should be no more than three parts:
        if (timeParts.length > 3) {
          throw new TimestampFormatException();
        }
        if (timeParts.length > 0) {
          hour = Integer.parseInt(timeParts[0]);
        }
        if (timeParts.length > 1) {
          minute = Integer.parseInt(timeParts[1]);
        }
        if (timeParts.length > 2) {
          // Need to handle both "00" and "00.000":
          String[] secondParts = timeParts[2].split("\\.");
          if (secondParts.length == 2) { // "00.000"
            second = Integer.parseInt(secondParts[0]);
          } else if (secondParts.length == 1) { // "00"
            second = Integer.parseInt(secondParts[0]);
          } else { // Something has gone wrong !!!
            throw new TimestampFormatException(
                "parseTimestamp: Error parsing Timestamp="
                    + timestampString);
          }
        }
      }
    } catch (NumberFormatException exception) {
      throw new TimestampFormatException(exception.getMessage());
    }
    LocalDate date = LocalDate.ofYearDay(year, dayOfYear);
    LocalTime time = LocalTime.of(hour, minute, second);

    return LocalDateTime.of(date, time);
  }
}
