import pandas as pd


def main(file_location: str):
    result = [{} for _ in range(0, 200)]
    lines = [
        "Feb 14 2021 13:32:53 NESTimer: /nebulastream/src/Services/QueryService.cpp(52)  [0x7f204d7fa700] [TRACE] : BDAPRO2Tracking: markAsRegistered - (queryId, nanoseconds) : (1, 1613309573409016510)",
        "Feb 14 2021 13:32:53 NESTimer: /nebulastream/src/Phases/GlobalQueryPlanUpdatePhase.cpp(65) RqstProc [0x7f208b7fe700] [TRACE] : BDAPRO2Tracking: markAsScheduling - (queryId, nanoseconds) : (1, 1613309573409927015)"]

    with open(file_location, "r") as f:
        lines = f.readlines()
    for line in lines:
        if "BDAPRO2Tracking: mark" in line:
            if "BDAPRO2Tracking: markAs" in line:
                after = line.split("BDAPRO2Tracking: markAs")[1]
            else:
                after = line.split("BDAPRO2Tracking: mark")[1]
            state_split = after.split("-")
            state = state_split[0].strip()
            metric_splits = state_split[1].split(":")[1].split(",")
            query_id = int(metric_splits[0].split("(")[1].strip())
            result[query_id - 1][state] = int(int(metric_splits[1].split(")")[0].strip()))
            print(f"Processed: {query_id}")

    df = pd.DataFrame.from_dict(result)
    df.to_csv("./results/4_fluctuating_flat_disabled_20_results.csv")


if __name__ == '__main__':
    main("./results/4_fluctuating_flat_disabled_20.log")
