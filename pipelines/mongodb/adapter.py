#!/usr/bin/env python3
"""MongoDB aggregation adapter for the NASA HTTP log project.

Raw records are streamed into a run-scoped MongoDB collection. MongoDB's
aggregation framework parses/materializes records and computes Q1-Q3. The
adapter writes the same JSONL contract as the other backends.
"""
import argparse
import json
import os
import sys
import time
import traceback
import uuid
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from etl_common.config import default_batch_size, default_input_files, load_config, load_dotenv, repo_path
from etl_common.staging import iter_records

INSERT_CHUNK = 5000
TOP_N = 20

# MongoDB executes this function for each raw document. All field validation
# and malformed reason selection happen in the database, not in Python.
PARSE_JS = r"""
function(raw) {
  function bad(reason) { return {valid:false, reason:reason}; }
  if (raw === "") return bad("BLANK_LINE");
  if (raw.indexOf("\u0000") !== -1) return bad("NUL_BYTE");
  var m = /^([^ \t]+)[ \t]+([^ \t]+)[ \t]+([^ \t]+)[ \t]+\[([^\]]*)\][ \t]+"([\s\S]*)"[ \t]+([^ \t]+)(?:[ \t]+([^ \t]+))?[ \t]*$(?![\s\S])/.exec(raw);
  if (!m) return bad("BAD_STRUCTURE");
  var host=m[1], ts=m[4], request=m[5], status=m[6], bytes=m[7];
  if (host === "-") return bad("BAD_HOST");
  var t=/^(\d{2})\/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-])(\d{2})(\d{2})$(?![\s\S])/.exec(ts);
  if (!t) return bad("BAD_TIMESTAMP");
  var months={Jan:1,Feb:2,Mar:3,Apr:4,May:5,Jun:6,Jul:7,Aug:8,Sep:9,Oct:10,Nov:11,Dec:12};
  var day=+t[1], month=months[t[2]], year=+t[3], hour=+t[4], minute=+t[5], second=+t[6], oh=+t[8], om=+t[9];
  var leap=(year%4===0 && (year%100!==0 || year%400===0));
  var mdays=[31,leap?29:28,31,30,31,30,31,31,30,31,30,31];
  if (year<1 || day<1 || day>mdays[month-1] || hour>23 || minute>59 || second>59 || oh>23 || om>59) return bad("BAD_TIMESTAMP");
  if (!/^[0-9]{3}$(?![\s\S])/.test(status)) return bad("BAD_STATUS");
  var sizeText="0";
  if (bytes !== undefined && bytes !== "-") {
    if (!/^[0-9]+$(?![\s\S])/.test(bytes)) return bad("BAD_BYTES");
    // Validate the signed 64-bit limit before MongoDB converts the decimal string.
    var normalized=bytes.replace(/^0+/, "") || "0", max="9223372036854775807";
    if (normalized.length>max.length || (normalized.length===max.length && normalized>max)) return bad("BAD_BYTES");
    sizeText=normalized;
  }
  var trimmed=request.replace(/^[ \t]+|[ \t]+$/g, "");
  var tokens=(trimmed==="" || trimmed==="-") ? [] : trimmed.split(/[ \t]+/);
  return {valid:true,reason:null,host:host,timestamp:ts,
    log_date:t[3]+"-"+("0"+month).slice(-2)+"-"+t[1],log_hour:t[4],
    http_method:tokens.length?tokens[0]:null,
    resource_path:(tokens.length>=2 && tokens.length<=3)?tokens[1]:null,
    protocol_version:tokens.length===3?tokens[2]:null,
    status_code:status,bytes_transferred:sizeText};
}
"""


def write_jsonl(path, rows):
    with Path(path).open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def parse_args():
    p=argparse.ArgumentParser(description="NASA HTTP log MongoDB aggregation adapter")
    p.add_argument("--batch-size", type=int)
    p.add_argument("--output-dir", required=False)
    p.add_argument("--input", action="append")
    p.add_argument("--debug-parsed", action="store_true")
    p.add_argument("--engine-info", action="store_true")
    return p.parse_args()


def pipeline(scope=None):
    stages=[]
    if scope is not None: stages.append({"$match":{"batch_id":scope}})
    stages += [{"$match":{"parsed.valid":True}}, {"$match":{"parsed.resource_path":{"$ne":None}}}]
    stages += [
        {"$group":{"_id":"$parsed.resource_path","request_count":{"$sum":1},
                    "total_bytes":{"$sum":"$parsed.bytes_transferred"},
                    "hosts":{"$addToSet":"$parsed.host"}}},
        {"$project":{"_id":0,"resource_path":"$_id","request_count":1,"total_bytes":1,
                      "distinct_host_count":{"$size":"$hosts"}}},
        {"$sort":{"request_count":-1,"resource_path":1}}, {"$limit":TOP_N},
    ]
    return stages


def scoped_rows(collection, batch_ids):
    daily=[]; top=[]; hourly=[]
    scopes=[*batch_ids, None]
    for scope in scopes:
        match={"parsed.valid":True}
        if scope is not None: match["batch_id"]=scope
        for row in collection.aggregate([
            {"$match":match},
            {"$group":{"_id":{"date":"$parsed.log_date","status":"$parsed.status_code"},
                        "request_count":{"$sum":1},"total_bytes":{"$sum":"$parsed.bytes_transferred"}}},
            {"$sort":{"_id.date":1,"_id.status":1}},
            {"$project":{"_id":0,"batch_id":{"$literal":scope},"log_date":"$_id.date",
                          "status_code":"$_id.status","request_count":1,"total_bytes":1}},
        ]): daily.append(row)
        for rank,row in enumerate(collection.aggregate(pipeline(scope), collation={"locale":"simple"}),1):
            row["batch_id"]=scope; row["rank"]=rank; top.append(row)
        for row in collection.aggregate([
            {"$match":match},
            {"$group":{"_id":{"date":"$parsed.log_date","hour":"$parsed.log_hour"},
                        "total":{"$sum":1},
                        "errors":{"$sum":{"$cond":[{"$and":[{"$gte":["$parsed.status_code",400]},{"$lte":["$parsed.status_code",599]}]},1,0]}},
                        "error_hosts":{"$addToSet":{"$cond":[{"$and":[{"$gte":["$parsed.status_code",400]},{"$lte":["$parsed.status_code",599]}]},"$parsed.host",None]}}}},
            {"$sort":{"_id.date":1,"_id.hour":1}},
            {"$project":{"_id":0,"batch_id":{"$literal":scope},"log_date":"$_id.date","log_hour":"$_id.hour",
                          "error_request_count":"$errors","total_request_count":"$total",
                          "error_rate":{"$divide":["$errors","$total"]},
                          "distinct_error_hosts":{"$subtract":[{"$size":"$error_hosts"},{"$cond":[{"$in":[None,"$error_hosts"]},1,0]}]}}},
        ]): hourly.append(row)
    return daily,top,hourly


def main():
    args=parse_args()
    if args.engine_info:
        try:
            import pymongo
            from pymongo import MongoClient
            client=MongoClient(os.getenv("MONGO_URI","mongodb://localhost:27017"),serverSelectionTimeoutMS=3000)
            print(json.dumps({"pipeline":"MongoDB","engine":"MongoDB aggregation framework","version":client.server_info()["version"],"pymongo":pymongo.version,"adapter":"pipelines/mongodb/adapter.py"},indent=2)); client.close(); return 0
        except Exception as e: print(f"MongoDB engine unavailable: {e}",file=sys.stderr); return 1
    try:
        try: from pymongo import MongoClient
        except ImportError as e: raise RuntimeError("MongoDB requires pymongo; run python3 -m pip install -r requirements.txt") from e
        load_dotenv(); cfg=load_config()
        inputs=[repo_path(p) for p in args.input] if args.input else default_input_files(cfg)
        batch_size=args.batch_size if args.batch_size is not None else default_batch_size(cfg)
        if batch_size<=0: raise ValueError("Batch size must be positive")
        if not args.output_dir: raise ValueError("--output-dir is required")
        missing=[str(p) for p in inputs if not p.is_file()]
        if missing: raise FileNotFoundError("Input file(s) not found: "+", ".join(missing))
        out=Path(args.output_dir).resolve(); out.mkdir(parents=True,exist_ok=False)
        uri=os.getenv("MONGO_URI","mongodb://localhost:27017")
        dbname=os.getenv("MONGO_DATABASE","nasa_log_analytics")
        client=MongoClient(uri,serverSelectionTimeoutMS=int(os.getenv("MONGO_TIMEOUT_MS","5000")))
        client.admin.command("ping")
        db=client[dbname]; suffix=uuid.uuid4().hex
        raw=db[f"raw_{suffix}"]; parsed=db[f"parsed_{suffix}"]
        timings=[]; counts=defaultdict(int); batch_ids=[]; samples=[]
        started=time.perf_counter(); docs=[]
        for rec in iter_records(inputs,batch_size):
            counts[rec.batch_id]+=1
            docs.append({"record_number":rec.record_number,"batch_id":rec.batch_id,"source_file":rec.source_file,"raw_line":rec.text})
            if len(docs)>=INSERT_CHUNK: raw.insert_many(docs,ordered=False); docs=[]
        if docs: raw.insert_many(docs,ordered=False)
        batch_ids=sorted(counts)
        timings.append({"phase_name":"ingest","duration_seconds":time.perf_counter()-started})
        started=time.perf_counter()
        raw.aggregate([{"$set":{"parsed":{"$function":{"body":PARSE_JS,"args":["$raw_line"],"lang":"js"}}}},
                       # Keep BSON integer fields exact (including signed 64-bit byte values).
                       {"$set":{"parsed.log_hour":{"$convert":{"input":"$parsed.log_hour","to":"int","onError":None,"onNull":None}},
                                "parsed.status_code":{"$convert":{"input":"$parsed.status_code","to":"int","onError":None,"onNull":None}},
                                "parsed.bytes_transferred":{"$convert":{"input":"$parsed.bytes_transferred","to":"long","onError":None,"onNull":None}}}},
                       {"$merge":{"into":parsed.name,"on":"_id","whenMatched":"replace","whenNotMatched":"insert"}}],allowDiskUse=True)
        parsed.create_index("batch_id"); parsed.create_index("record_number")
        timings.append({"phase_name":"parse","duration_seconds":time.perf_counter()-started})
        malformed_counts={r["_id"]:r["count"] for r in parsed.aggregate([
            {"$match":{"parsed.valid":False}},{"$group":{"_id":"$batch_id","count":{"$sum":1}}}])}
        batch_rows=[{"batch_id":b,"record_count":counts[b],"malformed_count":malformed_counts.get(b,0)} for b in batch_ids]
        samples=[{"source_file":r["source_file"],"record_number":r["record_number"],"batch_id":r["batch_id"],
                  "reason":r["parsed"]["reason"],"raw_line":r["raw_line"]}
                 for r in parsed.find({"parsed.valid":False},{"_id":0}).sort("record_number",1).limit(100)]
        started=time.perf_counter(); daily,top,hourly=scoped_rows(parsed,batch_ids)
        timings.append({"phase_name":"aggregation","duration_seconds":time.perf_counter()-started})
        outputs={"batches.jsonl":batch_rows,"daily_traffic.jsonl":daily,"top_resources.jsonl":top,
                 "hourly_errors.jsonl":hourly,"malformed_samples.jsonl":samples}
        if args.debug_parsed:
            fields=("host","timestamp","log_date","log_hour","http_method","resource_path","protocol_version","status_code","bytes_transferred")
            rows=[]
            for r in parsed.find({}, {"_id":0}).sort("record_number",1):
                p=r["parsed"]; row={k:r[k] for k in ("record_number","batch_id","source_file")}
                row.update({"valid":p["valid"],"reason":p["reason"]}); row.update({k:p.get(k) for k in fields}); rows.append(row)
            outputs["parsed_records.jsonl"]=rows
        for name,rows in outputs.items(): write_jsonl(out/name,rows)
        write_jsonl(out/"phase_timings.jsonl",timings)
        print(f"MongoDB completed: {len(batch_rows)} batches, {sum(counts.values())} records; outputs: {out}")
        raw.drop(); parsed.drop(); client.close()
    except Exception as exc:
        try: client.close()
        except Exception: pass
        print(f"MongoDB adapter failed: {exc}",file=sys.stderr); traceback.print_exc(file=sys.stderr); return 1
    return 0

if __name__ == "__main__": raise SystemExit(main())
