import pandas as pd


if __name__ == "__main__":
    left_join_side = pd.read_csv("stream_join_left.csv", header=None)   # SET name of left input file
    right_join_side = pd.read_csv("stream_join_right.csv", header=None) # SET name of right input file
    left_join_side.columns = ["value", "join_value", "ts"]
    right_join_side.columns = ["value", "join_value", "ts"]
    df_result = pd.DataFrame(columns=range(0,8))    # SET amount of columns in result record (2 + #leftColumns + #rightColumns)

    window_size = 15    # SET window size here
    window_slide = 5    # SET window slide here
    max_timestamp = max(left_join_side["ts"].max(), right_join_side["ts"].max())

    # create pairs of window start and end time
    windows = []
    start_times_windows = [0, 3]    # SET the deployment time of queries that the joinOperatorHandler handles. (basic case is just one query deployed at time 0)
    while start_times_windows[0] < max_timestamp:
        for i in range(0, len(start_times_windows)):
            windows.append((start_times_windows[i], start_times_windows[i] + window_size))
            start_times_windows[i] += 5

    # for each window start and end time. check through all tuples
    cnt = 0
    for window in windows:
        for _, tuple_left in left_join_side.iterrows():
            for _, tuple_right in right_join_side.iterrows():
                if window[0] <= tuple_left["ts"] < window[1] and window[0] <= tuple_right["ts"] < window[1]:    # left and right tuple are both in window
                    if tuple_left["join_value"] == tuple_right["join_value"]:                                   # the join condition of the left and right tuple evaluate to true
                        df_result.loc[cnt, :] = [window[0], window[1], tuple_left["value"], tuple_left["join_value"],
                                                 tuple_left["ts"], tuple_right["value"], tuple_right["join_value"],tuple_right["ts"]]
                        cnt += 1


    #df_result.to_csv("stream_join_sink_sliding.csv", header=None, index=None) # store csv file to your favourite location
    print(df_result)