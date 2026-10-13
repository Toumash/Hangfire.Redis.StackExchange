#!/usr/bin/env python3
import pathlib,subprocess,threading,time,datetime,json,urllib.request,socket,hashlib,http.server,uuid,os,argparse,tempfile,shutil,tarfile,io
from zoneinfo import ZoneInfo
REPO=pathlib.Path(__file__).resolve().parent.parent
parser=argparse.ArgumentParser(description="Real-store 40/80 renewal regression; isolated Docker resources only.")
parser.add_argument("--source-ref",help="Build an unchanged Git commit instead of the current library sources.")
parser.add_argument("--output",type=pathlib.Path,required=True,help="New directory for durable results.")
parser.add_argument("--sdk-image",default="mcr.microsoft.com/dotnet/sdk:10.0")
parser.add_argument("--cases",default="40,80")
args=parser.parse_args()
ROOT=args.output.resolve();ROOT.mkdir(parents=True)
HOLD_SECONDS=float(os.getenv("HOLD_SECONDS","50"))
PROFILE="run"
FAULT=os.getenv("REPRO_FAULT","none")
FAULT_AT=float(os.getenv("REPRO_FAULT_AT","20"))
assert FAULT in ["none","lease-loss","foreign-owner","pause"]
CASES=[int(x) for x in args.cases.split(",")]
assert all(x in [40,80] for x in CASES)
IMAGE=args.sdk_image
APP=ROOT/"app"
with tempfile.TemporaryDirectory(prefix="renewal-build-") as temporary:
 scratch=pathlib.Path(temporary);library=scratch/"library";probe=scratch/"probe"
 if args.source_ref:
  ref=subprocess.check_output(["git","rev-parse","--verify",args.source_ref+"^{commit}"],cwd=REPO,text=True).strip()
  archive=subprocess.check_output(["git","archive",ref],cwd=REPO)
  library.mkdir()
  with tarfile.open(fileobj=io.BytesIO(archive)) as tar:tar.extractall(library,filter="data")
 else:
  ref="working-tree"
  shutil.copytree(REPO/"Hangfire.Redis.StackExchange",library/"Hangfire.Redis.StackExchange",ignore=shutil.ignore_patterns("bin","obj"))
  shutil.copy2(REPO/"README.md",library/"README.md")
 shutil.copytree(REPO/"tools/RenewalAvailabilityProbe",probe,ignore=shutil.ignore_patterns("bin","obj"))
 sourceHashes={str(p.relative_to(library)):hashlib.sha256(p.read_bytes()).hexdigest()
  for p in (library/"Hangfire.Redis.StackExchange").rglob("*.cs")}
 with (ROOT/"build.log").open("w") as log:
  subprocess.run(["docker","run","--rm","-v",str(library)+":/library","-v",str(probe)+":/probe",
   "-v",str(ROOT)+":/out","-w","/probe",IMAGE,"dotnet","publish","RenewalAvailabilityProbe.csproj",
   "-c","Release","-o","/out/app","-p:RedisProjectPath=/library/Hangfire.Redis.StackExchange/Hangfire.Redis.StackExchange.csproj"],
   stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
EXPECTED_SHA=hashlib.sha256((APP/"Hangfire.Redis.StackExchange.dll").read_bytes()).hexdigest()
deps=json.loads((APP/"RenewalAvailabilityProbe.deps.json").read_text())["libraries"]
assert not any(k.startswith("SellersKit") for k in deps)
(ROOT/"build-proof.json").write_text(json.dumps(dict(sourceRef=ref,sourceSha256=sourceHashes,dllSha256=EXPECTED_SHA,sdkImage=IMAGE,
 library=[k for k in deps if k.startswith("Hangfire.Redis.StackExchange/")]),indent=2)+"\n")
def docker(*args):return subprocess.check_output(["docker",*args],text=True,timeout=30).strip()
def mapped(name,port):
 x=json.loads(docker("inspect",name));return int(x[0]["NetworkSettings"]["Ports"][str(port)+"/tcp"][0]["HostPort"])
class Delayed(http.server.BaseHTTPRequestHandler):
 def do_GET(self):
  time.sleep(HOLD_SECONDS);body=b'{"ok":true}'
  try:self.send_response(200);self.send_header("Content-Length",str(len(body)));self.end_headers();self.wfile.write(body)
  except (BrokenPipeError,ConnectionResetError):pass
 def log_message(self,*args):pass
server=http.server.ThreadingHTTPServer(("0.0.0.0",0),Delayed);server.daemon_threads=True
threading.Thread(target=server.serve_forever,daemon=True).start()
def redis(port,parts):
 with socket.create_connection(("127.0.0.1",port),timeout=2) as sock:
  sock.settimeout(2);wire=b"*"+str(len(parts)).encode()+b"\r\n"+b"".join(b"$"+str(len(p.encode())).encode()+b"\r\n"+p.encode()+b"\r\n" for p in parts);sock.sendall(wire);f=sock.makefile("rb")
  def read():
   line=f.readline(65536)
   if not line:raise EOFError()
   prefix=line[:1];value=line[1:-2]
   if prefix==b"$":
    n=int(value)
    if n<0:return None
    assert n<100000;data=f.read(n);assert f.read(2)==b"\r\n";return data
   if prefix==b"*":return [read() for _ in range(int(value))]
   if prefix==b"+":return value
   if prefix==b"-":raise RuntimeError("scratch Redis error")
   return int(value)
  return read()
results=[]
try:
 for count in CASES:
  stamp=uuid.uuid4().hex[:8];net="renewal-availability-"+stamp;rn=net+"-redis";wn=net+"-worker";case=ROOT/("case-"+PROFILE+"-"+str(count));case.mkdir()
  log=None;wait=None;handle=None;rows=[];faultApplied=None
  try:
   docker("network","create",net)
   docker("run","-d","--name",rn,"--network",net,"--network-alias","redis","-p","127.0.0.1::6379","redis@sha256:43afe4a9a43080e64dfa29699eef903fc9d54991fe8b03d57237c05bed2a396c")
   rp=mapped(rn,6379)
   for _ in range(50):
    try:redis(rp,["PING"]);break
    except Exception:time.sleep(.1)
   docker("run","-d","--name",wn,"--network",net,"--cpus=2","--memory=1000m","-e","REPRO_REDIS=redis:6379","-e","REPRO_PROVIDER=http://host.docker.internal:"+str(server.server_port)+"/wait","-e","REPRO_EXPECT_LOSS="+("1" if FAULT in ["lease-loss","foreign-owner"] else "0"),"-e","REPRO_KEYS=/out/keys.jsonl","-e","REPRO_SAME_KEY="+os.getenv("REPRO_SAME_KEY","0"),"-v",str(APP)+":/app:ro","-v",str(case)+":/out","-w","/app","-p","127.0.0.1::9090",IMAGE,"dotnet","/app/RenewalAvailabilityProbe.dll",str(count))
   mp=mapped(wn,9090);handle=(case/"host.log").open("w");log=subprocess.Popen(["docker","logs","-f",wn],stdout=handle,stderr=handle);wait=subprocess.Popen(["docker","wait",wn],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
   start=time.monotonic();seenHealthy=False
   while time.monotonic()-start<145 and wait.poll() is None:
    began=time.monotonic();row={"utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"elapsedMs":int((began-start)*1000)}
    try:
     with urllib.request.urlopen("http://127.0.0.1:"+str(mp)+"/metrics",timeout=2) as response:body=response.read(512001)
     assert len(body)<=512000;row["healthy"]=response.status==200;seenHealthy=True
    except Exception as e:row.update(healthy=False,errorType=type(e).__name__)
    row["requestMs"]=int((time.monotonic()-began)*1000);row["healthyPreviouslyObserved"]=seenHealthy
    try:
     keymap={}
     if (case/"keys.jsonl").exists():
      for line in (case/"keys.jsonl").read_text().splitlines():
       try:x=json.loads(line);keymap[x["key"]]=x["ownerSha256"]
       except ValueError:pass
     values=redis(rp,["MGET",*keymap]) if keymap else []
     if FAULT!="none" and faultApplied is None and time.monotonic()-start>=FAULT_AT and len(keymap)==count:
      if FAULT=="lease-loss":redis(rp,["DEL",*keymap])
      elif FAULT=="foreign-owner":
       for key in keymap:redis(rp,["SET",key,"foreign-owner","PX","60000"])
      elif FAULT=="pause":redis(rp,["CLIENT","PAUSE",os.getenv("REPRO_PAUSE_MS","3500"),"ALL"])
      else:raise ValueError("unknown local fault")
      faultApplied={"fault":FAULT,"utc":row["utc"],"keys":len(keymap),"elapsedMs":row["elapsedMs"]}
     row.update(keys=len(keymap),matchingOwners=sum(v is not None and hashlib.sha256(v).hexdigest().upper()==expected for v,expected in zip(values,keymap.values())),missingOwners=sum(v is None for v in values))
    except Exception as e:row["ownerReadErrorType"]=type(e).__name__
    rows.append(row);(case/"metrics-observer.json").write_text(json.dumps(rows,indent=2)+"\n")
    time.sleep(max(0,.5-(time.monotonic()-began)))
   forced=wait.poll() is None
   if forced:docker("kill",wn)
   exitCode=int(wait.communicate(timeout=10)[0].decode().strip());log.wait(timeout=10);handle.close();handle=None
   finalForeign=None
   if FAULT=="foreign-owner" and faultApplied:
    keymap={}
    for line in (case/"keys.jsonl").read_text().splitlines():
     x=json.loads(line);keymap[x["key"]]=x["ownerSha256"]
    values=redis(rp,["MGET",*keymap]);ttls=[redis(rp,["PTTL",key]) for key in keymap]
    finalForeign={"foreignOwnersUnchanged":sum(v==b"foreign-owner" for v in values),
     "minTtlMs":min(ttls),"maxTtlMs":max(ttls),"observedUtc":datetime.datetime.now(datetime.timezone.utc).isoformat()}
   events=[]
   for line in (case/"host.log").read_text().splitlines():
    try:events.append(json.loads(line))
    except ValueError:pass
   startup=next((x for x in events if x.get("kind")=="startup"),None);result=next((x for x in events if x.get("kind")=="result"),None);samples=[x for x in events if x.get("kind")=="sample"]
   failures=[r for r in rows if not r["healthy"] and r["healthyPreviouslyObserved"]]
   ownerOutages=[r for r in failures if r.get("keys")==count and r.get("matchingOwners")==count]
   now=datetime.datetime.now(datetime.timezone.utc)
   summary={"completedUtc":now.isoformat(),"completedWarsaw":now.astimezone(ZoneInfo("Europe/Warsaw")).isoformat(),"workers":count,"profile":PROFILE,"httpHoldSeconds":HOLD_SECONDS,"startup":startup,"result":result,"exitCode":exitCode,"forcedLocalCleanup":forced,"maxActive":max((x["active"] for x in samples),default=0),"maxPending":max((x["pending"] for x in samples),default=0),"maxPoolThreads":max((x["poolThreads"] for x in samples),default=0),"maxWorkingSet":max((x["workingSet"] for x in samples),default=0),"metricsFailuresAfterHealthy":len(failures),"outagesWithAllCorrectOwners":len(ownerOutages),"firstOutageWithAllCorrectOwners":ownerOutages[0] if ownerOutages else None,"faultApplied":faultApplied,"finalForeign":finalForeign,"dllSha256":EXPECTED_SHA,"limits":"library regression: real Hangfire/Redis/delayed HTTP/Kestrel; default pool and natural timers; key mutations only in labeled owned fault controls; no application E2E claim"}
   (case/"summary.json").write_text(json.dumps(summary,indent=2)+"\n");results.append(summary);(ROOT/("results-"+PROFILE+".json")).write_text(json.dumps(results,indent=2)+"\n");print(json.dumps(summary),flush=True)
  finally:
   if log and log.poll() is None:log.terminate()
   if wait and wait.poll() is None:wait.terminate()
   if handle:handle.close()
   for name in [wn,rn]:subprocess.run(["docker","rm","-f","-v",name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=20)
   subprocess.run(["docker","network","rm",net],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=20)
finally:server.shutdown();server.server_close()
checks=[]
for summary in results:
 case=ROOT/("case-"+PROFILE+"-"+str(summary["workers"]))
 events=[]
 for line in (case/"host.log").read_text().splitlines():
  try:events.append(json.loads(line))
  except ValueError:pass
 samples=[x for x in events if x.get("kind")=="sample"]
 result=summary["result"]
 resultTime=result.get("utc") if result else None
 rows=json.loads((case/"metrics-observer.json").read_text())
 def parsed(s):return datetime.datetime.fromisoformat(s.replace("Z","+00:00"))
 failures=[]
 activeSuccesses=0
 for row in rows:
  t=parsed(row["utc"])
  if resultTime and t>=parsed(resultTime):continue
  prior=[x for x in samples if parsed(x["utc"])<=t]
  if not prior or prior[-1]["active"]==0:continue
  age=(t-parsed(prior[-1]["utc"])).total_seconds()
  if row["healthy"]:activeSuccesses+=1
  elif row["healthyPreviouslyObserved"]:failures.append({**row,"sampleAgeSeconds":age})
 check=dict(workers=summary["workers"],exitCode=summary["exitCode"],result=result,
  activeMetricsSuccesses=activeSuccesses,activeMetricsFailures=failures,
  maxActive=summary["maxActive"],maxPending=summary["maxPending"],
  samplingLimit="0.5s point probes; the most recent independent active sample may be stale; not continuous availability")
 check["passed"]=(result is not None and summary["exitCode"]==0 and
   ((result.get("states",{}).get("Failed")==summary["workers"] and result["saved"]==0 and result["completed"]==0)
    if FAULT in ["lease-loss","foreign-owner"] else (result["succeeded"]==summary["workers"] and result["saved"]==summary["workers"])) and
   result["started"]==summary["workers"] and
   result["duplicates"]==0 and result.get("overlaps",0)==0 and result["minimumWorkersUnchanged"] and
   summary["startup"]["minimumWorkers"]==2 and summary["startup"]["processorCount"]==2 and summary["maxActive"]==(summary["workers"]//2 if os.getenv("REPRO_SAME_KEY")=="1" else summary["workers"]) and
   len(failures)==0 and activeSuccesses>=40 and
   (FAULT!="foreign-owner" or (summary.get("finalForeign",{}).get("foreignOwnersUnchanged")==summary["workers"] and
     summary["finalForeign"]["minTtlMs"]>0 and summary["finalForeign"]["maxTtlMs"]<55000)))
 checks.append(check)
(ROOT/("assertions-"+PROFILE+".json")).write_text(json.dumps(checks,indent=2)+"\n")
print(json.dumps({"assertions":[dict(workers=x["workers"],passed=x["passed"],activeMetricsFailures=len(x["activeMetricsFailures"])) for x in checks]}))
raise SystemExit(0 if all(c["passed"] for c in checks) else 1)
