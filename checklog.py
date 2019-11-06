from ast import literal_eval
from datetime import datetime

import geopy.distance
import pandas as pd

import sys

# Global settings
# pd.options.mode.chained_assignment = None  # ignore some warnings


class settings:
    # LOG filename
    filename = "log"

    # Detect movement
    MOVE_SPEED = 10 / 3600  # 10 miles/hour = 10/3600 miles/sec
    MOVE_TIME = 3600  # for checking idle status
    START_TIME = 900  # Should start within 15 mins

    # For timestamp checking
    NIGHT_START = 18  # UTC+0: 18:00~24:00
    NIGHT_END = 24

    # For battery checking: decreased 15% per hour
    BATTERY_LEVEL = 15  # 15%
    BATTERY_TIME = 3600  # 1hr

    # For upload log checking: upload within 20 mins
    UPLOAD_TIME = 1200  # 20 mins

    # For removing default coordinates
    IGNORE_COORD = [(0, 0), (-1, -1)]

    # For getting running intervals
    IDLE_TIME = 900


def timeformat(str):
    """Convert time format string to datetime format
    
    Arguments:
        str {string} -- Date format from raw log.
    
    Returns:
        string -- Datetime format string
    """
    str = str.replace("a.m.", "AM")
    str = str.replace("p.m.", "PM")
    str = str.replace("midnight", "12:00 AM")
    # add :00 on the hour
    if str.find(":") == -1:
        splited = str.split(" ")
        splited[-2] += ":00"
        str = " ".join(splited)
    return datetime.strptime(str, "%b. %d, %Y, %I:%M %p")


def readlog(filename):
    # Initialize, read data and do preprocessing
    with open(filename, "r", encoding="utf-8") as file:
        raw_data = [x.rstrip().split("\t") for x in file.readlines()]
    logdata = pd.DataFrame(raw_data[1:])
    logdata.columns = raw_data[0]
    # Convert Location format
    logdata["Location(Lat,Lng)"] = logdata["Location(Lat,Lng)"].apply(
        lambda x: literal_eval(x)
    )
    # Remove default location
    logdata = logdata[~logdata["Location(Lat,Lng)"].isin(settings.IGNORE_COORD)]
    # Convert log date to fit datetime object
    logdata["Date(UTC+0)"] = logdata["Date(UTC+0)"].apply(timeformat)
    # Remove percent sign for Battery
    logdata["Battery"] = logdata["Battery"].apply(lambda str: int(str.replace("%", "")))
    # Prevent duplicate checking
    logdata.insert(len(logdata.columns), "stop_checked", False)
    logdata.insert(len(logdata.columns), "start_checked", False)
    logdata.insert(len(logdata.columns), "should_start_checked", False)
    logdata.insert(len(logdata.columns), "upload_checked", False)
    logdata.insert(len(logdata.columns), "shouldnt_start_checked", False)
    logdata.insert(len(logdata.columns), "running_interval", False)
    return logdata


def time_delta(dateobj1, dateobj2):
    # returns delta in seconds
    return (dateobj1 - dateobj2).total_seconds()


def check_stop(logdata, index):
    current_log = logdata.iloc[index]
    if current_log["stop_checked"]:
        pass
    else:
        dist = 0
        last = current_log
        for i in range(index + 1, len(logdata)):  # check previous log
            if logdata.iloc[i]["Status"] in [
                "idle",
                "offline",
                "task_complete",
            ]:  # end of running status
                break

            # Set flag to prevent duplicate checking
            logdata.at[
                logdata["Log ID"] == logdata.iloc[i]["Log ID"], "stop_checked"
            ] = True

            # Save moved distance
            current = logdata.iloc[i - 1]["Location(Lat,Lng)"]
            prev = logdata.iloc[i]["Location(Lat,Lng)"]
            dist += geopy.distance.distance(current, prev).miles

            # Update last log
            last = logdata.iloc[i]

        time = time_delta(current_log["Date(UTC+0)"], last["Date(UTC+0)"])
        stopped = False
        # Find "stop" status after last "running" to check if should stop or should continue
        for i in range(index - 1, -1, -1):
            if logdata.iloc[i]["Status"] == "stop":
                stopped = True
                break
        if time != 0:
            if (dist / time) >= settings.MOVE_SPEED:
                # Should not stop, check if current task has stopped
                if stopped:
                    speed = dist / time * 3600
                    print(
                        "[STOP] Should NOT STOP at ID: %s~%s, Speed: %f miles/hr (during last hour)."
                        % (current_log["Log ID"], last["Log ID"], speed)
                    )
            elif (dist / time) < settings.MOVE_SPEED:
                # Should stop, check if current task has stopped
                if not stopped:
                    speed = dist / time * 3600
                    print(
                        "[STOP] Should STOP at ID: %s~%s, Speed: %f miles/hr (during last hour)."
                        % (current_log["Log ID"], last["Log ID"], speed)
                    )


def check_should_start(logdata, index):
    current_log = logdata.iloc[index]
    if current_log["start_checked"]:
        pass
    else:
        log_list = []
        for i in range(
            index, len(logdata)
        ):  # Find "idle" and "task_complete", to check if should start
            if logdata.iloc[i]["Status"] not in ["idle", "task_complete"]:
                break
            # Set flag to prevent duplicate check
            logdata.at[
                logdata["Log ID"] == logdata.iloc[i]["Log ID"], "start_checked"
            ] = True
            log_list.append(logdata.iloc[i])

        # Start parsing pre-starting status
        separate_list = []
        # separate log by time range to list of list
        for i in range(len(log_list)):
            current = log_list[i]
            current_list = [current]
            for j in range(i + 1, len(log_list)):
                if (
                    time_delta(current["Date(UTC+0)"], log_list[j]["Date(UTC+0)"])
                    >= settings.MOVE_TIME
                ):
                    break
                current_list.append(log_list[j])
            separate_list.append(current_list)

        # Parse each log list in separate list
        for l in separate_list:
            dist = 0
            time = time_delta(l[0]["Date(UTC+0)"], l[-1]["Date(UTC+0)"])
            if time == 0:
                continue  # ignore this list
            for i in range(len(l) - 1):
                # calculate moved distance
                dist += geopy.distance.distance(
                    l[i]["Location(Lat,Lng)"], l[i + 1]["Location(Lat,Lng)"]
                ).miles
            speed = dist / time
            if speed >= settings.MOVE_SPEED:
                # should start, check if task starts
                # get log location
                assert (
                    len(logdata.index[logdata["Log ID"] == l[0]["Log ID"]].tolist()) == 1
                ), "Error: Should not have duplicate log id"
                start_idx = logdata.index[logdata["Log ID"] == l[0]["Log ID"]].tolist()[0]
                # find first "starting" status
                start_task = False
                for j in range(start_idx, -1, -1):
                    if logdata.iloc[j]["Status"] == "start_mobileinsight":
                        if logdata.iloc[j]["should_start_checked"]:  # already checked
                            start_task = True
                            break
                        else:  # first starting status found, set flag and end checking
                            logdata.at[
                                logdata["Log ID"] == logdata.iloc[j]["Log ID"],
                                "should_start_checked",
                            ] = True
                            start_task = True
                            break
                if not start_task:
                    print(
                        "[START] Should START at %s~%s, Speed: %f miles/hr (during last hour)."
                        % (l[0]["Log ID"], l[-1]["Log ID"], speed * 3600)
                    )
                """
                We dont need to handle speed < MOVE_SPEED, check_shouldnt_start will handle it.
                """


def check_shouldnt_start(logdata, index):
    if logdata.iloc[index]["shouldnt_start_checked"] == True:
        pass
    else:
        last_start_mob_idx = None
        for i in range(index, len(logdata)):
            if logdata.iloc[i]["Status"] not in ["start_mobileinsight"]:
                break
            last_start_mob_idx = i  # get last index of "start_mobileinsight"
            logdata.at[
                logdata["Log ID"] == logdata.iloc[i]["Log ID"], "shouldnt_start_checked"
            ] = True
        # Calculate average speed
        dist = 0
        idle_list = []
        for i in range(last_start_mob_idx + 1, len(logdata)):
            if (
                logdata.iloc[i]["Status"] not in ["task_complete", "idle"]
                or time_delta(  # out of range
                    logdata.iloc[last_start_mob_idx]["Date(UTC+0)"],
                    logdata.iloc[i]["Date(UTC+0)"],
                )
                >= settings.MOVE_TIME
            ):
                break
            # idle status before start_mobileinsight, check average speed
            idle_list.append(logdata.iloc[i])
        for i in range(len(idle_list) - 1):
            dist += geopy.distance.distance(
                idle_list[i]["Location(Lat,Lng)"], idle_list[i + 1]["Location(Lat,Lng)"]
            ).miles
        time = time_delta(idle_list[0]["Date(UTC+0)"], idle_list[-1]["Date(UTC+0)"])
        if time != 0:
            speed = dist / time
            if speed < settings.MOVE_SPEED:
                # Should not start running
                print(
                    "[START] Should NOT START at: %s, Speed: %f miles/hr (during last hour)."
                    % (logdata.iloc[last_start_mob_idx]["Log ID"], speed * 3600)
                )


def check_battery(logdata, index, prev_start_id, prev_end_id):
    current_log = logdata.iloc[index]
    battery_list = []
    for j in range(index, len(logdata)):
        if (
            logdata.iloc[j]["Status"] == "offline"
            or time_delta(current_log["Date(UTC+0)"], logdata.iloc[j]["Date(UTC+0)"])
            > settings.BATTERY_TIME
        ):
            break
        battery_list.append(logdata.iloc[j])
    # Find decreasing intervals
    dec_list = []
    indicator_arr = [False for _ in range(len(battery_list))]
    for i in range(len(battery_list) - 1, 0, -1):
        if battery_list[i]["Battery"] > battery_list[i - 1]["Battery"]:
            tmp_list = []
            for j in range(i, -1, -1):
                if battery_list[j]["Battery"] < battery_list[j - 1]["Battery"]:
                    if not indicator_arr[j]:
                        tmp_list.append(battery_list[j])
                        dec_list.append(tmp_list)
                    indicator_arr[j] = True
                    break
                if not indicator_arr[j]:
                    tmp_list.append(battery_list[j])
                indicator_arr[j] = True
    # Check every decreasing intervals
    for l in dec_list:
        battery_decreased = l[0]["Battery"] - l[-1]["Battery"]
        if battery_decreased >= settings.BATTERY_LEVEL:
            if l[0]["Log ID"] not in prev_end_id and l[-1]["Log ID"] not in prev_start_id:
                print(
                    "[BATTERY] %s~%s Decreased: %d%%"
                    % (l[-1]["Log ID"], l[0]["Log ID"], battery_decreased)
                )
                prev_end_id.append(l[0]["Log ID"])
                prev_start_id.append(l[-1]["Log ID"])
    return prev_start_id, prev_end_id


def check_upload(logdata, index):
    uploaded = False
    current_log = logdata.iloc[index]
    for i in range(index - 1, -1, -1):
        if (
            time_delta(logdata.iloc[i]["Date(UTC+0)"], current_log["Date(UTC+0)"])
            > settings.UPLOAD_TIME  # out of range
            or logdata.iloc[i]["Status"] == "task_complete"  # another task
        ):
            break
        if logdata.iloc[i]["Upload Status"] == "complete":
            uploaded = True
            break
    for i in range(index + 1, len(logdata)):  # set flag to prevent duplicate checking
        if logdata.iloc[i]["Status"] != "task_complete":
            break
        logdata.at[
            logdata["Log ID"] == logdata.iloc[i]["Log ID"], "upload_checked"
        ] = True
    if not uploaded:
        print("[UPLOAD] Log was not uploaded: %s" % current_log["Log ID"])


def check_timestamp(logdata, index):
    current_log = logdata.iloc[index]
    time_hr = current_log["Date(UTC+0)"].hour
    time_min = current_log["Date(UTC+0)"].minute
    if time_hr >= settings.NIGHT_START and time_hr < settings.NIGHT_END:
        print("[TIME] %s, %d:%02d" % (current_log["Log ID"], time_hr, time_min))


def running_interval(logdata, index):
    if logdata.iloc[index]["running_interval"] == True:
        pass
    else:
        current_log = logdata.iloc[index]
        running_logs = []
        for i in range(index, len(logdata)):
            if logdata.iloc[i]["Status"] == "start_task":
                # End of running interval
                break
            if logdata.iloc[i]["Status"] != "running":
                # Check if is still in running state
                if (
                    time_delta(current_log["Date(UTC+0)"], logdata.iloc[i]["Date(UTC+0)"])
                    >= settings.IDLE_TIME
                ):
                    break
            logdata.at[
                logdata["Log ID"] == logdata.iloc[i]["Log ID"], "running_interval"
            ] = True
            running_logs.append(logdata.iloc[i])
        print(
            "[RUNNING STATE] Run at %s~%s, From: %s, To: %s"
            % (
                current_log["Log ID"],
                running_logs[-1]["Log ID"],
                running_logs[-1]["Date(UTC+0)"],
                running_logs[0]["Date(UTC+0)"],
            )
        )


def main():
    if len(sys.argv) > 1:
        print("Reading: " + sys.argv[1])
        logdata = readlog(sys.argv[1])
    else:
        # default log file name
        logdata = readlog(settings.filename)

    prev_end_id = []  # For removing duplicate battery messages
    prev_start_id = []

    for i in range(len(logdata)):
        current_log = logdata.iloc[i]

        # Check running task: whether should stop or not
        if current_log["Status"] == "running":
            running_interval(logdata, i)
            check_stop(logdata, i)

        # Check "IDLE" status: whether should start or not
        if current_log["Status"] in ["idle", "task_complete"]:
            check_should_start(logdata, i)

        # If start, check whether should start or not
        if current_log["Status"] == "start_mobileinsight":
            check_shouldnt_start(logdata, i)

        # Check battery level per hour
        if current_log["Status"] != "offline":
            prev_start_id, prev_end_id = check_battery(
                logdata, i, prev_start_id, prev_end_id
            )

        # Check upload after task complete
        if not current_log["upload_checked"] and current_log["Status"] == "task_complete":
            check_upload(logdata, i)

        # Check timestamp: should not run during night time
        # check_timestamp(logdata, i)


if __name__ == "__main__":
    main()
