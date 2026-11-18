package nosql.mapreduce;

public class ParserTest {

    public static void main(String[] args) {

        String[] testLines = {
            "uplherc.upl.com - - [01/Aug/1995:00:00:01 -0400] \"GET /images/logo.gif HTTP/1.0\" 200 1234",
            "host.example.com - - [01/Aug/1995:10:20:30 -0400] \"GET /index.html HTTP/1.0\" 404 -",
            "host.example.com - - [01/Aug/1995:10:20:30 -0400] \"GET\" 200 100",
            "host.example.com - - [invalid timestamp] \"GET /test HTTP/1.0\" 200 100",
            "",
            "host.example.com - - [01/Aug/1995:10:20:30 -0400] \"GET /test HTTP/1.0\" ABC 100"
        };

        for (int i = 0; i < testLines.length; i++) {

            ParsedRecord record = LogParser.parse(
                i + 1,
                1,
                "test.log",
                testLines[i]
            );

            System.out.println("----------------------------------");
            System.out.println("Record Number: " + record.recordNumber);
            System.out.println("Valid: " + record.valid);
            System.out.println("Reason: " + record.reason);

            if (record.valid) {
                System.out.println("Host: " + record.host);
                System.out.println("Timestamp: " + record.timestamp);
                System.out.println("Date: " + record.logDate);
                System.out.println("Hour: " + record.logHour);
                System.out.println("Method: " + record.httpMethod);
                System.out.println("Path: " + record.resourcePath);
                System.out.println("Protocol: " + record.protocolVersion);
                System.out.println("Status: " + record.statusCode);
                System.out.println("Bytes: " + record.bytesTransferred);
            }
        }
    }
}