package nosql.pig;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parses the ISO-8859-1 content already decoded by the shared stager. */
final class LogParser {
    private static final Pattern OUTER = Pattern.compile(
        "^([^ \\t]+)[ \\t]+([^ \\t]+)[ \\t]+([^ \\t]+)[ \\t]+\\[([^\\]]*)\\]"
        + "[ \\t]+\"(.*)\"[ \\t]+([^ \\t]+)(?:[ \\t]+([^ \\t]+))?[ \\t]*$",
        Pattern.DOTALL
    );
    private static final Pattern TIMESTAMP = Pattern.compile(
        "([0-9]{2})/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/([0-9]{4}):"
        + "([0-9]{2}):([0-9]{2}):([0-9]{2}) [+-]([0-9]{2})([0-9]{2})"
    );
    private static final List<String> MONTHS = List.of(
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    );
    private static final Pattern STATUS = Pattern.compile("[0-9]{3}");
    private static final Pattern BYTES = Pattern.compile("[0-9]+");
    private static final Pattern REQUEST_EDGES = Pattern.compile("^[ \\t]+|[ \\t]+$");
    private static final Pattern REQUEST_SEPARATOR = Pattern.compile("[ \\t]+");

    private LogParser() {}

    // Matches the field order declared in ParseLine. Invalid values remain null.
    static Object[] parse(String text) {
        if (text.isEmpty()) {
            return invalid("BLANK_LINE");
        }
        if (text.indexOf('\0') >= 0) {
            return invalid("NUL_BYTE");
        }
        Matcher outer = OUTER.matcher(text);
        if (!outer.matches()) {
            return invalid("BAD_STRUCTURE");
        }
        String host = outer.group(1);
        if ("-".equals(host)) {
            return invalid("BAD_HOST");
        }
        String timestamp = outer.group(4);
        Matcher time = TIMESTAMP.matcher(timestamp);
        if (!time.matches()) {
            return invalid("BAD_TIMESTAMP");
        }
        int year = number(time, 3);
        int hour = number(time, 4);
        if (year == 0 || hour > 23 || number(time, 5) > 59 || number(time, 6) > 59
                || number(time, 7) > 23 || number(time, 8) > 59) {
            return invalid("BAD_TIMESTAMP");
        }
        LocalDate date;
        try {
            date = LocalDate.of(year, MONTHS.indexOf(time.group(2)) + 1, number(time, 1));
        } catch (DateTimeException ex) {
            return invalid("BAD_TIMESTAMP");
        }
        String status = outer.group(6);
        if (!STATUS.matcher(status).matches()) {
            return invalid("BAD_STATUS");
        }
        long size = 0;
        String bytes = outer.group(7);
        if (bytes != null && !"-".equals(bytes)) {
            if (!BYTES.matcher(bytes).matches()) {
                return invalid("BAD_BYTES");
            }
            try {
                size = Long.parseLong(bytes);
            } catch (NumberFormatException ex) {
                return invalid("BAD_BYTES");
            }
        }
        String request = REQUEST_EDGES.matcher(outer.group(5)).replaceAll("");
        String[] parts = request.isEmpty() || "-".equals(request)
            ? new String[0] : REQUEST_SEPARATOR.split(request, 4);
        return new Object[] {
            true, null, host, timestamp, date.toString(), hour,
            parts.length > 0 ? parts[0] : null,
            parts.length == 2 || parts.length == 3 ? parts[1] : null,
            parts.length == 3 ? parts[2] : null,
            Integer.parseInt(status), size
        };
    }

    private static int number(Matcher time, int index) {
        return Integer.parseInt(time.group(index));
    }

    private static Object[] invalid(String reason) {
        Object[] values = new Object[11];
        values[0] = false;
        values[1] = reason;
        return values;
    }
}
