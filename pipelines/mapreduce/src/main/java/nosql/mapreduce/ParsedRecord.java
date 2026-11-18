
package nosql.mapreduce;

public class ParsedRecord {
    public long recordNumber;
    public int batchId;
    public String sourceFile;
    public boolean valid;
    public String reason;

    public String host;
    public String timestamp;
    public String logDate;
    public Integer logHour;
    public String httpMethod;
    public String resourcePath;
    public String protocolVersion;
    public Integer statusCode;
    public Long bytesTransferred;

    public ParsedRecord(long recordNumber, int batchId, String sourceFile) {
        this.recordNumber = recordNumber;
        this.batchId = batchId;
        this.sourceFile = sourceFile;
        this.valid = false;
    }

    public static ParsedRecord malformed(
            long recordNumber,
            int batchId,
            String sourceFile,
            String reason
    ) {
        ParsedRecord r = new ParsedRecord(recordNumber, batchId, sourceFile);
        r.valid = false;
        r.reason = reason;
        return r;
    }
}
