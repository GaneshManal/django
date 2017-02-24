import os


def poll(meta={}, state={}):
    # use meta and state as you would prefer
    if not state:
        state["index"] = 0
    else:
        state["index"] +=  1
    return [{"poll": str(os.getpid())}], state
