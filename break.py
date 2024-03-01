import lldb
desired_func = "NES::Runtime::Execution::Operators::StreamJoinOperatorHandler::checkAndTriggerWindows"
def only(frame, bp_loc, dict):
    thread = frame.thread
    for frame in thread.frames:
        if frame.name == desired_func:
            return True
    return False
