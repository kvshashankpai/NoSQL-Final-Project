"""MongoDB aggregation pipelines: parsing (docs/PARSING_RULES.md) and Q1-Q3.

All parsing, validation and aggregation happens inside MongoDB. Python only
builds these pipeline definitions and serializes the results.

Requires MongoDB 5.0+ ($getField).
"""

# Same patterns as docs/PARSING_RULES.md sections 4 and 5.
# tests/test_mongodb_pipeline.py checks they match the doc.
OUTER_PATTERN = (
    r'^([^ \t]+)[ \t]+([^ \t]+)[ \t]+([^ \t]+)[ \t]+\[([^\]]*)\][ \t]+"(.*)"'
    r'[ \t]+([^ \t]+)(?:[ \t]+([^ \t]+))?[ \t]*$'
)
TIMESTAMP_PATTERN = (
    r'^(\d{2})/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/(\d{4}):'
    r'(\d{2}):(\d{2}):(\d{2}) ([+-])(\d{2})(\d{2})$'
)
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_DIGITS = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
MAX_BIGINT_TEXT = "9223372036854775807"
TOP_N = 20
SAMPLE_LIMIT = 100


def _at(arr, i):
    return {"$arrayElemAt": [arr, i]}


def _int(expr):
    return {"$toInt": expr}


def parse_stages(batch_id, parsed_collection):
    """Pipeline over raw_lines for one batch: parse, classify and $merge into parsed.

    Each output document: _id (record number), batch_id, source_file, valid,
    reason, the nine fields (null when malformed), and raw_line (only kept for
    malformed records, for the samples).
    """
    is_blank = {"$eq": ["$line", ""]}
    has_nul = {"$gte": [{"$indexOfCP": ["$line", "\u0000"]}, 0]}
    # Python's "." never meets a newline here (records have no LF), so the
    # "s" option only guarantees that "." also matches CR, as in Python and Java.
    outer = {"$regexFind": {"input": "$line", "regex": OUTER_PATTERN, "options": "s"}}

    return [
        {"$match": {"batch_id": batch_id}},
        {"$addFields": {"_blank": is_blank, "_nul": has_nul}},
        {"$addFields": {"_o": {"$cond": [{"$or": ["$_blank", "$_nul"]}, None, outer]}}},
        {"$addFields": {"_c": "$_o.captures"}},
        {"$addFields": {
            "_host": _at("$_c", 0),
            "_ts": _at("$_c", 3),
            "_req": _at("$_c", 4),
            "_status": _at("$_c", 5),
            "_bytes": _at("$_c", 6),
        }},
        {"$addFields": {
            "_t": {"$cond": [{"$eq": [{"$type": "$_ts"}, "string"]},
                             {"$regexFind": {"input": "$_ts", "regex": TIMESTAMP_PATTERN}}, None]},
        }},
        {"$addFields": {"_tc": "$_t.captures"}},
        {"$addFields": {
            "_day": _int(_at("$_tc", 0)),
            "_mon": {"$add": [{"$indexOfArray": [MONTHS, _at("$_tc", 1)]}, 1]},
            "_year": _int(_at("$_tc", 2)),
            "_hh": _int(_at("$_tc", 3)),
            "_mi": _int(_at("$_tc", 4)),
            "_ss": _int(_at("$_tc", 5)),
            "_oh": _int(_at("$_tc", 7)),
            "_om": _int(_at("$_tc", 8)),
        }},
        {"$addFields": {
            "_leap": {"$or": [
                {"$and": [{"$eq": [{"$mod": ["$_year", 4]}, 0]},
                          {"$ne": [{"$mod": ["$_year", 100]}, 0]}]},
                {"$eq": [{"$mod": ["$_year", 400]}, 0]}]},
        }},
        {"$addFields": {
            "_mdays": {"$switch": {"branches": [
                {"case": {"$in": ["$_mon", [1, 3, 5, 7, 8, 10, 12]]}, "then": 31},
                {"case": {"$in": ["$_mon", [4, 6, 9, 11]]}, "then": 30},
                {"case": {"$eq": ["$_mon", 2]}, "then": {"$cond": ["$_leap", 29, 28]}},
            ], "default": 0}},
        }},
        {"$addFields": {
            "_ts_ok": {"$and": [
                {"$ne": ["$_t", None]},
                {"$gte": ["$_year", 1]},
                {"$gte": ["$_day", 1]}, {"$lte": ["$_day", "$_mdays"]},
                {"$lte": ["$_hh", 23]}, {"$lte": ["$_mi", 59]}, {"$lte": ["$_ss", 59]},
                {"$lte": ["$_oh", 23]}, {"$lte": ["$_om", 59]},
            ]},
            "_status_ok": {"$and": [
                {"$eq": [{"$type": "$_status"}, "string"]},
                {"$regexMatch": {"input": {"$ifNull": ["$_status", ""]}, "regex": "^[0-9]{3}$"}},
            ]},
            # Significant digits of the bytes token (leading zeros removed).
            "_bsig": {"$cond": [
                {"$eq": [{"$type": "$_bytes"}, "string"]},
                _at({"$ifNull": [{"$getField": {"field": "captures", "input":
                    {"$regexFind": {"input": "$_bytes", "regex": "^0*([0-9]+)$"}}}}, [None]]}, 0),
                None]},
        }},
        {"$addFields": {
            "_bytes_ok": {"$or": [
                {"$eq": ["$_bytes", None]},
                {"$eq": ["$_bytes", "-"]},
                {"$and": [
                    {"$eq": [{"$type": "$_bsig"}, "string"]},
                    {"$or": [
                        {"$lt": [{"$strLenCP": "$_bsig"}, 19]},
                        {"$and": [{"$eq": [{"$strLenCP": "$_bsig"}, 19]},
                                  {"$lte": ["$_bsig", MAX_BIGINT_TEXT]}]},
                    ]},
                ]},
            ]},
        }},
        {"$addFields": {
            "reason": {"$switch": {"branches": [
                {"case": "$_blank", "then": "BLANK_LINE"},
                {"case": "$_nul", "then": "NUL_BYTE"},
                {"case": {"$eq": ["$_o", None]}, "then": "BAD_STRUCTURE"},
                {"case": {"$eq": ["$_host", "-"]}, "then": "BAD_HOST"},
                {"case": {"$not": ["$_ts_ok"]}, "then": "BAD_TIMESTAMP"},
                {"case": {"$not": ["$_status_ok"]}, "then": "BAD_STATUS"},
                {"case": {"$not": ["$_bytes_ok"]}, "then": "BAD_BYTES"},
            ], "default": None}},
        }},
        {"$addFields": {"valid": {"$eq": ["$reason", None]}}},
        {"$addFields": {
            "_tok": {"$let": {
                "vars": {"r": {"$trim": {"input": {"$ifNull": ["$_req", ""]}, "chars": " \t"}}},
                "in": {"$cond": [
                    {"$in": ["$$r", ["", "-"]]}, [],
                    {"$map": {"input": {"$regexFindAll": {"input": "$$r", "regex": "[^ \t]+"}},
                              "as": "m", "in": "$$m.match"}}]},
            }},
        }},
        {"$addFields": {"_ntok": {"$size": "$_tok"}}},
        {"$project": {
            "_id": 1,
            "batch_id": 1,
            "source_file": 1,
            "valid": 1,
            "reason": 1,
            "host": {"$cond": ["$valid", "$_host", None]},
            "timestamp": {"$cond": ["$valid", "$_ts", None]},
            "log_date": {"$cond": ["$valid", {"$concat": [
                _at("$_tc", 2), "-", _at(MONTH_DIGITS, {"$subtract": ["$_mon", 1]}), "-", _at("$_tc", 0)]},
                None]},
            "log_hour": {"$cond": ["$valid", "$_hh", None]},
            "http_method": {"$cond": [{"$and": ["$valid", {"$gte": ["$_ntok", 1]}]}, _at("$_tok", 0), None]},
            "resource_path": {"$cond": [{"$and": ["$valid", {"$gte": ["$_ntok", 2]}, {"$lte": ["$_ntok", 3]}]},
                                        _at("$_tok", 1), None]},
            "protocol_version": {"$cond": [{"$and": ["$valid", {"$eq": ["$_ntok", 3]}]}, _at("$_tok", 2), None]},
            "status_code": {"$cond": ["$valid", _int("$_status"), None]},
            "bytes_transferred": {"$cond": ["$valid", {"$cond": [
                {"$in": ["$_bytes", [None, "-"]]}, {"$toLong": 0}, {"$toLong": "$_bsig"}]}, None]},
            "raw_line": {"$cond": ["$valid", "$$REMOVE", "$line"]},
        }},
        {"$merge": {"into": parsed_collection, "on": "_id", "whenMatched": "fail", "whenNotMatched": "insert"}},
    ]


def _scope_match(batch_id):
    return {"valid": True} if batch_id is None else {"batch_id": batch_id, "valid": True}


def batch_stats_pipeline(batch_id):
    return [
        {"$match": {"batch_id": batch_id}},
        {"$group": {"_id": None, "record_count": {"$sum": 1},
                    "malformed_count": {"$sum": {"$cond": ["$valid", 0, 1]}}}},
    ]


def daily_traffic_pipeline(batch_id):
    return [
        {"$match": _scope_match(batch_id)},
        {"$group": {"_id": {"log_date": "$log_date", "status_code": "$status_code"},
                    "request_count": {"$sum": 1},
                    "total_bytes": {"$sum": "$bytes_transferred"}}},
        {"$project": {"_id": 0, "log_date": "$_id.log_date", "status_code": "$_id.status_code",
                      "request_count": 1, "total_bytes": 1}},
        {"$sort": {"log_date": 1, "status_code": 1}},
    ]


def top_resources_pipeline(batch_id):
    match = _scope_match(batch_id)
    match["resource_path"] = {"$ne": None}
    return [
        {"$match": match},
        # Distinct hosts without building host sets: group by (path, host) first.
        {"$group": {"_id": {"p": "$resource_path", "h": "$host"},
                    "c": {"$sum": 1}, "b": {"$sum": "$bytes_transferred"}}},
        {"$group": {"_id": "$_id.p", "request_count": {"$sum": "$c"},
                    "total_bytes": {"$sum": "$b"}, "distinct_host_count": {"$sum": 1}}},
        # Strings compare by binary UTF-8 order under MongoDB's simple collation.
        {"$sort": {"request_count": -1, "_id": 1}},
        {"$limit": TOP_N},
        # Rank = position in that order. ($documentNumber allows only one sort key.)
        {"$group": {"_id": None, "rows": {"$push": "$$ROOT"}}},
        {"$unwind": {"path": "$rows", "includeArrayIndex": "i"}},
        {"$project": {"_id": 0, "rank": {"$toInt": {"$add": ["$i", 1]}}, "resource_path": "$rows._id",
                      "request_count": "$rows.request_count", "total_bytes": "$rows.total_bytes",
                      "distinct_host_count": "$rows.distinct_host_count"}},
        {"$sort": {"rank": 1}},
    ]


def hourly_errors_pipeline(batch_id):
    is_error = {"$and": [{"$gte": ["$status_code", 400]}, {"$lte": ["$status_code", 599]}]}
    return [
        {"$match": _scope_match(batch_id)},
        {"$group": {"_id": {"d": "$log_date", "h": "$log_hour", "host": "$host"},
                    "total": {"$sum": 1}, "errors": {"$sum": {"$cond": [is_error, 1, 0]}}}},
        {"$group": {"_id": {"d": "$_id.d", "h": "$_id.h"},
                    "total_request_count": {"$sum": "$total"},
                    "error_request_count": {"$sum": "$errors"},
                    "distinct_error_hosts": {"$sum": {"$cond": [{"$gt": ["$errors", 0]}, 1, 0]}}}},
        {"$project": {"_id": 0, "log_date": "$_id.d", "log_hour": "$_id.h",
                      "error_request_count": 1, "total_request_count": 1,
                      "error_rate": {"$divide": ["$error_request_count", "$total_request_count"]},
                      "distinct_error_hosts": 1}},
        {"$sort": {"log_date": 1, "log_hour": 1}},
    ]


def malformed_samples_pipeline():
    return [
        {"$match": {"valid": False}},
        {"$sort": {"_id": 1}},
        {"$limit": SAMPLE_LIMIT},
        {"$project": {"_id": 0, "source_file": 1, "record_number": "$_id", "batch_id": 1,
                      "reason": 1, "raw_line": 1}},
    ]


def parsed_records_pipeline():
    return [
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "record_number": "$_id", "batch_id": 1, "source_file": 1,
                      "valid": 1, "reason": 1, "host": 1, "timestamp": 1, "log_date": 1,
                      "log_hour": 1, "http_method": 1, "resource_path": 1, "protocol_version": 1,
                      "status_code": 1, "bytes_transferred": 1}},
    ]
