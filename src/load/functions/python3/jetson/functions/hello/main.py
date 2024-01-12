from time import time

cold = True

def main(args):
    start = time()
    global cold
    was_cold = cold
    cold = False
    name = args.get("name", "stranger")
    greeting = "Hello " + name + " from python!"
    end = time()
    return {"body": {"greeting": greeting, "cold":was_cold, "start":start, "end":end, "latency": end-start} }
