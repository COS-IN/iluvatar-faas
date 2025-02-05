
def format_line(*args):
  formatter = "{},"*len(args)
  chars = list(formatter)
  chars[-1] = '\n'
  return "".join(chars).format(*args)

def write_trace(trace, metadata, metadata_cols, trace_save_pth, metadata_save_pth):
    with open(trace_save_pth, "w") as f:
        f.write(format_line("func_name", "invoke_time_ms"))
        for func_name, time_ms in trace:
            f.write(format_line(func_name, int(time_ms)))

    with open(metadata_save_pth, "w") as f:
        f.write(format_line(*metadata_cols))
        for line in metadata:
            f.write(format_line(*line))