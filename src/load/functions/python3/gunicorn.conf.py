import sys
def when_ready(server):
    server.log.info("MGK_GUN_READY_KMG")
    server.log.error("MGK_GUN_READY_KMG")
    print("MGK_GUN_READY_KMG", file=sys.stderr)
