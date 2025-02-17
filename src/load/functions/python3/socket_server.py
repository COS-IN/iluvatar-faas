import os, sys, traceback, json
from datetime import datetime
from math import ceil
from socketserver import UnixStreamServer, StreamRequestHandler, ThreadingMixIn

DRIVER="libgpushare.so"
def driver_enabled() -> bool:
    has_gpu = os.path.isfile("/usr/bin/nvidia-smi")
    if "LD_PRELOAD" in os.environ and DRIVER in os.environ["LD_PRELOAD"]:
        return has_gpu
    return False

gpushare = None
if driver_enabled():
    import ctypes
    gpushare = ctypes.CDLL(DRIVER, mode=os.RTLD_GLOBAL)
    gpushare.total_cuda_allocations.restype = ctypes.c_double

FORMAT="%Y-%m-%d %H:%M:%S:%f+%z"
cold=True
#  Store import error of user code
import_msg = None
try:
    # import user code on startup
    from main import main
except:
    import_msg = traceback.format_exc()



def index():
    global import_msg
    if import_msg is not None:
        return json.dumps({"Status":"code import error", "user_error":import_msg})
    else:
        return json.dumps({"Status":"OK"})

def to_dev():
    if gpushare is None:
        return json.dumps({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold})
    return json.dumps({"Status":gpushare.madviseToDevice()})

def off_dev():
    if gpushare is None:
        return json.dumps({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold})
    return json.dumps({"Status":gpushare.madviseToHost()})

def prefetch_host():
    if gpushare is None:
        return json.dumps({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold})
    return json.dumps({"Status":gpushare.prefetchToHost()})

def prefetch_dev():
    if gpushare is None:
        return json.dumps({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold})
    return json.dumps({"Status":gpushare.prefetchToDevice()})

def prefetch_stream_host():
    if gpushare is None:
        return json.dumps({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold})
    return json.dumps({"Status":gpushare.prefetchStreamToHost()})
def prefetch_stream_dev():
    if gpushare is None:
        return json.dumps({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold})
    return json.dumps({"Status":gpushare.prefetchStreamToDevice()})

def gpu_mem():
    if gpushare is None:
        return json.dumps({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold})
    return json.dumps({"gpu_allocation_mb": ceil(gpushare.total_cuda_allocations())})

def append_metadata(user_ret, start, end, was_cold, success=True):
    duration = (end - start).total_seconds()
    ret = {"start": datetime.strftime(start, FORMAT), "end": datetime.strftime(end, FORMAT), "was_cold":was_cold, "duration_sec": duration}
    if gpushare is not None:
        ret["gpu_allocation_mb"] = ceil(gpushare.total_cuda_allocations())
    if success:
        ret["user_result"] = json.dumps(user_ret)
    else:
        ret["user_error"] = json.dumps(user_ret)
    return json.dumps(ret)

def invoke(args_bytes):
    global cold
    was_cold = cold
    cold = False
    if import_msg is not None:
        # If your custom main function from above was used (failed import), we return its results here
        return append_metadata(import_msg, start=datetime.now(), end=datetime.now(), was_cold=was_cold, success=False)

    try:
        json_input = json.loads(args_bytes.decode('utf-8'))
    except Exception as e:
        # Usually comes from malformed json arguments
        return json.dumps({"platform_error":str(e), "was_cold":was_cold})

    if gpushare is not None:
        gpushare.ensure_on_device()
        gpushare.check_async_prefetch()

    start = datetime.now()
    try:
        ret = main(json_input)
        end = datetime.now()
        # wrap user output with our own recorded information
        return append_metadata(ret, start, end, was_cold)
    except Exception as e:
        # User code failed, report the error with the rest of our information
        end = datetime.now()
        return append_metadata(e, start, end, was_cold, success=False)

socket_pth = os.environ.get("__IL_SOCKET", "/iluvatar/sockets/sock")
print("SOCKET:", socket_pth)

class Handler(StreamRequestHandler):
    def handle(self):
        while True:
            msg = self.rfile.read(2*8) # commands are 2 x 64 bit values
            cmd = int.from_bytes(msg[0:7], sys.byteorder, signed=False)
            size = int.from_bytes(msg[8:15], sys.byteorder, signed=False)
            ret = 0
            if cmd == 0:
                # check
                ret = index()
            elif cmd == 1:
                # invoke
                args = self.rfile.read(size)
                print(args)
                ret = invoke(args)
            elif cmd == 2:
                # to device
                ret = prefetch_stream_dev()
            elif cmd == 3:
                # from device
                ret = prefetch_stream_host()
            if type(ret) == str:
                ret = ret.encode()
            elif type(ret) == int:
                ret = bytes(ret)
            bytes_meta_send = len(ret).to_bytes(length=8, byteorder=sys.byteorder, signed=False)
            for b in bytes_meta_send:
                print("BYTE:", b)
            self.wfile.write(bytes_meta_send)
            self.wfile.write(ret)

class ThreadedUnixStreamServer(ThreadingMixIn, UnixStreamServer):
    pass

with ThreadedUnixStreamServer(socket_pth, Handler) as server:
    server.serve_forever()