from ast import literal_eval
from datetime import datetime

import geopy.distance
import pandas as pd

import sys


class settings:
    # LOG filename
    filename = "sample_log"

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

    idle_state = [
        "idle",
        "offline",
        "download_complete",
        "downloading",
        "task_complete",
        "mobileinsight_likely_dead",
    ]
    running_state = ["start_task", "running", "start_mobileinsight"]


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
    str = str.replace("noon", "12:00 PM")
    # add :00 on the hour
    if str.find(":") == -1:
        splited = str.split(" ")
        splited[-2] += ":00"
        str = " ".join(splited)
    return datetime.strptime(str, "%b. %d, %Y, %I:%M %p")


def time_delta(dateobj1, dateobj2):
    # returns delta in seconds
    return abs((dateobj1 - dateobj2).total_seconds())


def readlog(filename):
    print("Reading: %s" % filename)
    print("================================")

    # Initialize, read data and do preprocessing
    with open(filename, "r", encoding="utf-8") as file:
        raw_data = [x.rstrip().split("\t") for x in file.readlines()]
    logdata = pd.DataFrame(raw_data[1:])
    logdata.columns = raw_data[0]
    logdata = logdata[
        [
            "Log ID",
            "Battery",
            "Location(Lat,Lng)",
            "Status",
            "Upload Status",
            "Date(UTC+0)",
        ]
    ]
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
    return logdata.iloc[::-1].reset_index().drop(["index"], axis=1)


def is_in_table(table, last_elem):
    for _, v in table.items():
        if v["list"][-1]["Log ID"] == last_elem["Log ID"]:
            return True
    return False


def table_insert(table, item):
    for _, v in table.items():
        # if out of range, not append
        if (
            time_delta(item["Date(UTC+0)"], v["time"]) < settings.IDLE_TIME
        ):  # is within time range
            v["list"].append(item)
    if not is_in_table(table, item):
        # create new loglist and insert to table
        table[item["Log ID"]] = {"time": item["Date(UTC+0)"], "list": [item]}

    return table


def check_idle(table):
    should_start_running = False
    # check every list in dict
    for _, v in table.items():
        time = time_delta(v["time"], v["list"][-1]["Date(UTC+0)"])
        time = 1 if time == 0 else time  # prevent divide by zero
        # calculate distance
        dist = 0
        for elem in range(1, len(v["list"])):
            dist += geopy.distance.distance(
                v["list"][elem]["Location(Lat,Lng)"],
                v["list"][elem - 1]["Location(Lat,Lng)"],
            ).miles

        speed = dist / time
        if speed >= settings.MOVE_SPEED:
            # should start running
            should_start_running = True

        elif speed < settings.MOVE_SPEED:
            # should not start running
            should_start_running = False

    # return last interval's status
    return should_start_running, speed * 3600


def check_stop(table):
    should_stop_running = False
    for _, v in table.items():
        time = time_delta(v["time"], v["list"][-1]["Date(UTC+0)"])
        time = 1 if time == 0 else time
        dist = 0
        for elem in range(1, len(v["list"])):
            dist += geopy.distance.distance(
                v["list"][elem]["Location(Lat,Lng)"],
                v["list"][elem - 1]["Location(Lat,Lng)"],
            ).miles
        speed = dist / time
        if speed < settings.MOVE_SPEED:
            # should stop running
            should_stop_running = True

        elif speed >= settings.MOVE_SPEED:
            should_stop_running = False

    return should_stop_running, speed * 3600


def perf_evaluate(perf_list):
    # get last stop status position
    for i in range(len(perf_list) - 1, -1, -1):
        if perf_list[i]["Status"] not in settings.idle_state:
            perf_list = perf_list[: i + 1]
            break
    # get idle time and total running time
    start_idle = None
    idle_time = 0
    for i in range(1, len(perf_list)):
        if (
            perf_list[i - 1]["Status"] not in settings.idle_state
            and perf_list[i]["Status"] in settings.idle_state
        ):
            start_idle = perf_list[i]

        if (
            start_idle is not None
            and perf_list[i]["Status"] not in settings.idle_state
            and perf_list[i - 1]["Status"] in settings.idle_state
        ):
            idle_time += time_delta(
                perf_list[i - 1]["Date(UTC+0)"], start_idle["Date(UTC+0)"]
            )
            start_idle = None

    total_run_time = time_delta(
        perf_list[0]["Date(UTC+0)"], perf_list[-1]["Date(UTC+0)"]
    )
    return idle_time, total_run_time


def main():
    if len(sys.argv) > 1:
        logdata = readlog(sys.argv[1])
    else:
        # default log file name
        logdata = readlog(settings.filename)

    idle_list_table = {}
    should_start_running = False

    running_list_table = {}
    should_stop_running = False

    log_upload_timer = None
    log_uploaded = False

    perf_eval = False
    perf_list = []
    inactive_time = 0
    start_idle = None

    total_idle = 0
    total_run = 0

    for i in range(len(logdata)):
        prev = logdata.iloc[0] if i == 0 else logdata.iloc[i - 1]
        current = logdata.iloc[i]

        # ================= Check should not Start =================
        if current["Status"] in settings.idle_state:
            idle_list_table = table_insert(idle_list_table, current)

        # status changed from idle to active, clear table and summary
        if (
            current["Status"] == "start_mobileinsight"
            and prev["Status"] in settings.idle_state
        ):
            should_start_running, speed = check_idle(idle_list_table)
            idle_list_table = {}

        # if start running, check if indeed should start
        if (
            current["Status"] == "start_mobileinsight"
            and prev["Status"] in settings.idle_state  # prevent duplicate check
            and not should_start_running
        ):
            print(
                "[START] Should NOT START at %s, Speed: %f miles/hr (during last hour)."
                % (current["Log ID"], speed)
            )

        # ================= Check should not stop =================
        if current["Status"] in settings.running_state:
            running_list_table = table_insert(running_list_table, current)

        # status changed from running to stopped
        if current["Status"] == "stop" and prev["Status"] in settings.running_state:
            should_stop_running, speed = check_stop(running_list_table)
            running_list_table = {}

        if (
            current["Status"] == "stop"
            and prev["Status"] in settings.running_state  # prevent duplicate check
            and not should_stop_running
        ):
            print(
                "[STOP] Should NOT STOP at %s, Speed: %f miles/hr (during last hour)."
                % (current["Log ID"], speed)
            )

        # ================= Check Upload after task_complete =================
        if (
            current["Status"] == "task_complete"
        ):  # log should upload within "settings.UPLOAD_TIME" minutes
            log_upload_timer = current
            log_uploaded = False

        if log_upload_timer is not None:
            passed_time = time_delta(
                current["Date(UTC+0)"], log_upload_timer["Date(UTC+0)"]
            )
            if (
                current["Upload Status"] == "complete"
                and passed_time < settings.UPLOAD_TIME
            ):
                log_uploaded = True

            if (
                passed_time >= settings.UPLOAD_TIME
                or current["Status"] == "start_mobileinsight"  # a new task has started
                or i == len(logdata) - 1  # end of log
            ):
                if not log_uploaded:
                    print(
                        "[UPLOAD] Log was NOT uploaded: %s" % log_upload_timer["Log ID"]
                    )
                # clear timer
                log_upload_timer = None
                log_uploaded = False

        # TODO: battery check
        # TODO: should start but not start immediately
        # TODO: should stop but not stop immediately

        # ================= Performance evaluation =================

        if (
            current["Status"] in settings.idle_state
            and prev["Status"] not in settings.idle_state
            and start_idle is None
        ):
            start_idle = current

        if current["Status"] in settings.idle_state and start_idle is not None:
            inactive_time = time_delta(
                current["Date(UTC+0)"], start_idle["Date(UTC+0)"]
            )

        if (
            current["Status"] == "start_mobileinsight"
            and prev["Status"] in settings.idle_state
        ):
            # end of idle
            start_idle = None

        if (
            not perf_eval
            and current["Status"] == "start_mobileinsight"
            and prev["Status"] in settings.idle_state
        ):
            perf_eval = True

        if perf_eval:
            perf_list.append(current)
            if inactive_time >= settings.IDLE_TIME or (
                i == len(logdata) - 1  # end of log
            ):
                # get running interval, start evaluate
                perf_idle, perf_total = perf_evaluate(perf_list)
                total_idle += perf_idle
                total_run += perf_total
                perf_eval = False
                perf_list = []
                inactive_time = 0
                start_idle = None

        # end of performance evaluation

        # if inactive for too long, stop performance evaluation

    # performance evaluation
    total_run = 1 if total_run == 0 else total_run
    print("Performance: %f" % ((total_run - total_idle) / total_run))


if __name__ == "__main__":
    main()

