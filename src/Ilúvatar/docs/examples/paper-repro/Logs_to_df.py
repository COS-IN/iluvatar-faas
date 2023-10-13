import os
import pandas as pd
import json
import math
import pickle

############################################################

def function_name_to_paper(name: str) -> str:
  """
  Tries to convert a functon name into one suitable for the paper
  It will raise an error if one could not be found
  """
  map_dict = {"hello":"hello", "gzip_compression":"gzip", "pyaes":"AES", "dd":"dd", "lin_pack":"lin_pack", 
              "float_operation":"float", "chameleon":"web", "json_dumps_loads":"json", "cnn_image_classification":"cnn", 
              "image_processing":"image", "video_processing":"video", "model_training":"ml_train", "lookbusy":"bubble"}

  if name in map_dict:
    return map_dict[name]
  if "." in name:
    split_name = name.split(".")
    for part in split_name:
      if part in map_dict:
        return map_dict[part]
  if "-" in name:
    split_name = name.split("-")
    for part in split_name:
      if part in map_dict:
        return map_dict[part]
  raise Exception(f"Could not convert function name '{name}' to a paper-appropriate one")

class Logs_to_df():
    log_loc = '.'
    #standard filenames for now 
    worker_log = 'worker_worker1.log'
    docker_log = 'docker.log'

    cpu_log = 'cpu.csv'
    proc_log = 'process.log'

    # These are the main df outputs 
    worker_df = None
    process_cpu_df = None
    cpu_df = None
    proc_df = None
    status_df = None

    ##################################################
    
    fndict = None # dictionary for original name to short name conversion
    full_principals = True # Adds control-plane and system as 2 additional columns. Idle is different? 
    extra_principals = ["cp"]
    fnlist = []
    debug_timestamps = False
    mc_type = "server" # "desk", "laptop"
    first_invoke = None
    output_type = "full" # indiv, marginal, total
    
    ############################################### 
    
    def __init__(self, log_loc, mc_type='server'):
        """ location of logs, and the machine type """ 
        self.log_loc = log_loc
        self.mc_type = mc_type 

    ##################################################

    def get_fn_index(self, fn):
        # This is not needed but leaving here for future usecase maybe
        if self.fndict is not None:
            i = self.fnlist.index( self.fndict[fn] )
        else:
            i = self.fnlist.index(fn)

        return i

    ##################################################
    
    def worker_log_to_df(self):
      INVOKE_TARGET = "iluvatar_worker_library::services::containers::containerd::containerdstructs"
      INVOKE_NAME = "ContainerdContainer::invoke"  

      INVOKE_TARGET_S = "iluvatar_worker_library::services::containers::simulator::simstructs"
      INVOKE_NAME_S = "SimulatorContainer::invoke"  

      running = {}
      worker_log = os.path.join(self.log_loc, self.worker_log)
      if not os.path.exists(worker_log):
        print(f"Warning: Worker logs not available: {worker_log}")
        return
      def timestamp(log):
        return pd.to_datetime(log["timestamp"])

      def invoke_start(log):
        fqdn = log["span"]["fqdn"]
        tid = log["span"]["tid"]
        t = timestamp(log)
        running[tid] = (t, fqdn)

      def invoke_end(log):
        tid = log["span"]["tid"]
        stop_t = timestamp(log)
        start_t, fqdn = running[tid]
        duration = stop_t - start_t
        return duration, start_t, stop_t

      worker_df_list = []
      worker_status_list = []
      with open(worker_log, 'r') as f:
        while True:
          line = f.readline()
          if line == "":
            break
          try:
              log = json.loads(line)
          except Exception as e:
              print(e)
              print(line)
              continue 
        
          if log["fields"]["message"] == "new" or log["fields"]["message"] == "close":
              target = log["target"] == INVOKE_TARGET or log["target"] == INVOKE_TARGET_S
              span = log["span"]["name"] == INVOKE_NAME or log["span"]["name"] == INVOKE_NAME_S

          if log["fields"]["message"] == "new" and target and span:
              # invocation starting
            invoke_start(log)
            if self.first_invoke is None:
              self.first_invoke = timestamp(log)
          if log["fields"]["message"] == "close" and target and span:
            # invocation concluded
            fqdn = log["span"]["fqdn"]
            tid = log["span"]["tid"]
            try:
              duration, start_t, stop_t = invoke_end(log)
            except:
              pass
            worker_df_list.append((start_t, stop_t, tid, fqdn))

          if log["fields"]["message"] == "current load status":
            ts = timestamp(log)
            status = json.loads(log["fields"]["status"])
            mem_pct = (float(status["used_mem"]) / float(status["total_mem"])) * 100
            if status["cpu_sy"] == None:
                continue
            sys_cpu_pct = status["cpu_us"] + status["cpu_sy"] + status["cpu_wa"]
            # cpu_freq_mean = np.array(status["kernel_cpu_freqs"]).mean()
            load_avg = status["load_avg_1minute"] / status["num_system_cores"]
            data = (ts, status["cpu_queue_len"], mem_pct, sys_cpu_pct, load_avg, status["num_running_funcs"], status["num_containers"], status["cpu_us"], status["cpu_sy"], status["cpu_wa"])
            worker_status_list.append(data)

      worker_df = pd.DataFrame(worker_df_list, columns=['fn_start','fn_end','tid','fqdn'])
      self.status_df = pd.DataFrame.from_records(worker_status_list, columns=["timestamp", "queue_len", "mem_pct", "sys_cpu_pct", "load_avg", "num_running_funcs", "num_containers", "cpu_us", "cpu_sy", "cpu_wa"], index=["timestamp"])
      self.worker_df = worker_df

    ##################################################

    def cpu_to_df(self):
        cpu_log = os.path.join(self.log_loc, self.cpu_log)
        if not os.path.exists(cpu_log):
          print(f"Warning: Worker logs not available: {cpu_log}")
          return
        idf = pd.read_csv(cpu_log, parse_dates=['timestamp'])
        idf = idf.set_index('timestamp')
        idf = idf.astype(float)
        self.cpu_df = idf

    ##################################################

    def process_df(self):
        proc_log = os.path.join(self.log_loc, self.proc_log)
        try:
            idf = pd.read_csv(proc_log, parse_dates=['timestamp'])
        except FileNotFoundError:
            print(f"Warning: Process logs not available: {proc_log}")
            return 
        idf = idf.set_index('timestamp')

        self.proc_df = idf

    ##################################################

    def normalize_times(self):
      times = [self.status_df.index[0]]
      if self.proc_df is not None:
        times.append(self.proc_df.index[0])
      earliest_time = min(times)
      self.first_invoke = (self.first_invoke - earliest_time).total_seconds() / 60

      self.status_df["minute"] = self.status_df.index.to_series().apply(lambda x: (x-earliest_time).total_seconds() / 60)
      self.status_df["second"] = self.status_df.index.to_series().apply(lambda x: math.floor((x-earliest_time).total_seconds()))

      if self.proc_df is not None: 
        self.proc_df["minute"] = self.proc_df.index.to_series().apply(lambda x: (x-earliest_time).total_seconds() / 60)
        self.proc_df["second"] = self.proc_df.index.to_series().apply(lambda x: math.floor((x-earliest_time).total_seconds()))

    ##################################################

    def process_all_logs(self):
        """ Top-level function """ 
        self.worker_log_to_df()
        self.cpu_to_df()
        self.process_df()

        self.normalize_times()

    ##################################################

    def save_dfs(self):
        """ Top-level function """ 
        dfs = [ self.worker_df ]
        names = [ 'worker_df' ]
        
        for i in range(0,len(dfs)):
            f_pkl = self.log_loc + "/" + names[i] + ".pkl"
            f_csv = self.log_loc + "/" + names[i] + ".csv"
            with open( f_pkl, 'wb' ) as f:
                pickle.dump( dfs[i], f, protocol=pickle.HIGHEST_PROTOCOL )
                dfs[i].to_csv( f_csv ) 

    ############################################################
    #######################  END    ############################
    ############################################################
