import pandas as pd

def function_name(log) -> str:
  return log['span']['name']

def span_name(log) -> str:
  return f"{log['target']}::{log['span']['name']}"

def convert_time_to_ms(time: str):
  for i, c in enumerate(time[::-1]):
    if c.isnumeric():
      break
  unit = time[-i:]
  time = time[:-i]
  # print(i, unit, time)
  if unit == "ns":
    return float(time) / 1000000.0
  if unit == "µs":
    return float(time) / 1000.0
  elif unit == "ms":
    return float(time)
  elif unit == "s":
    return float(time) * 1000.0
  else:
    raise Exception(f"unknown unit of time: '{unit}' from '{time}'")

def span_time_ms(log, both: bool = False):
  try:
    work = log["fields"]["time.busy"]
    wait = log["fields"]["time.idle"]
  except Exception as e:
    print("span_time_ms ERROR:", e)
    print(log)
    raise e
  work = convert_time_to_ms(work)
  wait = convert_time_to_ms(wait)
  if both:
    return work + wait
  return work

def get_tid(log):
  try:
    if "fields" in log and "tid" in log["fields"]:
      return log["fields"]["tid"]
    elif "span" in log:
      return log["span"]["tid"]
    else:
      raise Exception("unknown format")
  except Exception as e:
    print("get_tid ERROR:", e)
    print(log)
    # exit(1)
    return None

def parse_date(string):
  # 2022-10-28 09:20:07.68160776
  return pd.to_datetime(string, format="%Y-%m-%d %H:%M:%S.%f")

def short_span_name(log):
  if type(log) is dict:
    full_name = span_name(log)
  elif type(log) is str:
    full_name = log
  else:
    raise Exception(f"unknown type to gen 'short_span_name': {type(log)}; {log}")
  splits = full_name.split("::")
  return "::".join(splits[-2:])

def span_function(log):
  if type(log) is dict:
    return log['span']['name']
  elif type(log) is str:
    return log.split("::")[-1]

def is_child(parent_log, check_log, exit_t):
  if parse_date(check_log["timestamp"]) > exit_t or parse_date(parent_log["timestamp"]) > parse_date(check_log["timestamp"]):
    return False
  if short_span_name(parent_log) == short_span_name(check_log):
    return False

  parent_fn = span_function(parent_log)
  if function_name(parent_log) == "sync_invocation" and function_name(check_log) == "enqueue_new_invocation":
    # hack because the API and invoker class have a shared function name
    # so that name will be in `enqueue`'s parent spans, but not associated
    return get_tid(parent_log) == get_tid(check_log)

  if function_name(parent_log) == "sync_invocation" and function_name(check_log) == "spawn_tokio_worker":
    return get_tid(parent_log) == get_tid(check_log)

  # hack because the span is not being correctly sent through the invoker queue
  if function_name(parent_log) == "spawn_tokio_worker" and short_span_name(check_log) == "invoker_trait::invocation_worker_thread":
    # they are "parent/child" if crossing the invoke queue boundary
    return get_tid(parent_log) == get_tid(check_log)

  if "spans" in check_log:
    if len(check_log["spans"]) <= 0:
      return False
    else:
      newest_parent = check_log["spans"][-1]
      return newest_parent["name"] == parent_fn
  return False