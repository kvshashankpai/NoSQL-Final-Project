package nosql.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;

/** Splits only the framing tabs. Tabs and CR inside the original line are data. */
public final class ParseLine extends EvalFunc<Tuple> {
    @Override
    public Tuple exec(Tuple input) throws IOException {
        String framed = (String) input.get(0);
        int first = framed.indexOf('\t');
        int second = first < 0 ? -1 : framed.indexOf('\t', first + 1);
        if (first < 1 || second <= first + 1) {
            throw new IOException("Invalid staged record framing");
        }
        String raw = framed.substring(second + 1);
        List<Object> fields = new ArrayList<>(14);
        fields.add(Long.parseLong(framed.substring(0, first)));
        fields.add(framed.substring(first + 1, second));
        fields.add(raw);
        fields.addAll(Arrays.asList(LogParser.parse(raw)));
        return TupleFactory.getInstance().newTupleNoCopy(fields);
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            return Utils.getSchemaFromString(
                "record:tuple(record_number:long, source_file:chararray, raw_line:chararray, "
                + "valid:boolean, reason:chararray, host:chararray, timestamp:chararray, "
                + "log_date:chararray, log_hour:int, http_method:chararray, resource_path:chararray, "
                + "protocol_version:chararray, status_code:int, bytes_transferred:long)"
            );
        } catch (FrontendException ex) {
            throw new IllegalStateException("Invalid parser schema", ex);
        }
    }
}
