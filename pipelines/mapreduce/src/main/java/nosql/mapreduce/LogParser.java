
package nosql.mapreduce;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {

    private static final Pattern OUTER_PATTERN = Pattern.compile(
        "^([^ \\t]+)[ \\t]+([^ \\t]+)[ \\t]+([^ \\t]+)[ \\t]+\\[([^\\]]*)\\][ \\t]+\"([\\s\\S]*)\"[ \\t]+([^ \\t]+)(?:[ \\t]+([^ \\t]+))?[ \\t]*$"
    );

    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile(
        "^(\\d{2})/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2}) ([+-])(\\d{2})(\\d{2})$"
    );

    private static final Pattern STATUS_PATTERN = Pattern.compile("^\\d{3}$");
    private static final Pattern BYTES_PATTERN = Pattern.compile("^\\d+$");

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
        new DateTimeFormatterBuilder()
            .appendPattern("dd/MMM/uuuu:HH:mm:ss")
            .toFormatter(Locale.US)
            .withResolverStyle(ResolverStyle.STRICT);

    private static final long MAX_BYTES = Long.MAX_VALUE;

    public static ParsedRecord parse(
            long recordNumber,
            int batchId,
            String sourceFile,
            String line
    ) {
        ParsedRecord result = new ParsedRecord(recordNumber, batchId, sourceFile);

        if (line == null || line.isEmpty()) {
            return ParsedRecord.malformed(
                recordNumber, batchId, sourceFile, "BLANK_LINE"
            );
        }

        if (line.indexOf('\0') >= 0) {
            return ParsedRecord.malformed(
                recordNumber, batchId, sourceFile, "NUL_BYTE"
            );
        }

        Matcher outer = OUTER_PATTERN.matcher(line);

        if (!outer.matches()) {
            return ParsedRecord.malformed(
                recordNumber, batchId, sourceFile, "BAD_STRUCTURE"
            );
        }

        String host = outer.group(1);
        String timestamp = outer.group(4);
        String request = outer.group(5);
        String statusToken = outer.group(6);
        String bytesToken = outer.group(7);

        if ("-".equals(host)) {
            return ParsedRecord.malformed(
                recordNumber, batchId, sourceFile, "BAD_HOST"
            );
        }

        LocalDateTime parsedDateTime = parseTimestamp(timestamp);

        if (parsedDateTime == null) {
            return ParsedRecord.malformed(
                recordNumber, batchId, sourceFile, "BAD_TIMESTAMP"
            );
        }

        if (!STATUS_PATTERN.matcher(statusToken).matches()) {
            return ParsedRecord.malformed(
                recordNumber, batchId, sourceFile, "BAD_STATUS"
            );
        }

        long bytes = 0L;

        if (bytesToken != null && !"-".equals(bytesToken)) {
            if (!BYTES_PATTERN.matcher(bytesToken).matches()) {
                return ParsedRecord.malformed(
                    recordNumber, batchId, sourceFile, "BAD_BYTES"
                );
            }

            try {
                bytes = Long.parseLong(bytesToken);
            } catch (NumberFormatException e) {
                return ParsedRecord.malformed(
                    recordNumber, batchId, sourceFile, "BAD_BYTES"
                );
            }

            if (bytes < 0 || bytes > MAX_BYTES) {
                return ParsedRecord.malformed(
                    recordNumber, batchId, sourceFile, "BAD_BYTES"
                );
            }
        }

        String method = null;
        String path = null;
        String protocol = null;

        if (request != null && !request.equals("-")) {
            String trimmed = request.replaceAll("^[ \\t]+|[ \\t]+$", "");

            if (!trimmed.isEmpty()) {
                String[] tokens = trimmed.split("[ \\t]+");

                if (tokens.length >= 1) {
                    method = tokens[0];
                }

                if (tokens.length == 2) {
                    path = tokens[1];
                } else if (tokens.length == 3) {
                    path = tokens[1];
                    protocol = tokens[2];
                }
            }
        }

        result.host = host;
        result.timestamp = timestamp;
        result.logDate = parsedDateTime.toLocalDate().toString();
        result.logHour = parsedDateTime.getHour();
        result.httpMethod = method;
        result.resourcePath = path;
        result.protocolVersion = protocol;
        result.statusCode = Integer.parseInt(statusToken);
        result.bytesTransferred = bytes;
        result.valid = true;
        result.reason = null;

        return result;
    }

    private static LocalDateTime parseTimestamp(String timestamp) {
        Matcher matcher = TIMESTAMP_PATTERN.matcher(timestamp);

        if (!matcher.matches()) {
            return null;
        }

        int offsetHours = Integer.parseInt(matcher.group(8));
        int offsetMinutes = Integer.parseInt(matcher.group(9));

        if (offsetHours > 23 || offsetMinutes > 59) {
            return null;
        }

        try {
            LocalDateTime dateTime = LocalDateTime.parse(
                timestamp.substring(0, 20),
                DATE_TIME_FORMATTER
            );

            LocalDate date = dateTime.toLocalDate();

            if (date.getYear() < 1) {
                return null;
            }

            return dateTime;
        } catch (Exception e) {
            return null;
        }
    }
}
