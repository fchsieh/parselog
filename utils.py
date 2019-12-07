import logging
import sys
from ast import literal_eval
from datetime import datetime

import geopy.distance
import pandas as pd


class settings:
    # LOG filename
    filename = "sample_log"

    # Detect movement
    MOVE_SPEED = 10 / 3600  # 10 miles/hour = 10/3600 miles/sec
    MOVE_TIME = 3600  # for checking idle status
    START_TIME = 900  # Should start within 15 mins
    TRIGGER_TIME = 360  # Should stop immediately if move under the threshold(6 mins)

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

    # define different log states
    idle_state = [
        "idle",
        "offline",
        "download_complete",
        "downloading",
        "task_complete",
        "mobileinsight_likely_dead",
    ]
    running_state = ["start_task", "running", "start_mobileinsight"]
    stop_state = ["stop", "mobileinsight_likely_dead", "offline", "idle"]


class colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    ENDC = "\033[0m"


def setcolor(color, str):
    if color == "RED":
        clr = colors.RED
    elif color == "GREEN":
        clr = colors.GREEN
    elif color == "YELLOW":
        clr = colors.YELLOW
    return clr + str + colors.ENDC


def logger(classes, str):
    if classes == "Performance":
        log_level = colors.GREEN
    else:
        log_level = colors.RED
    print("[" + log_level + classes + colors.ENDC + "] " + str)


def time_delta(dateobj1, dateobj2):
    # returns delta in seconds
    return abs((dateobj1 - dateobj2).total_seconds())


class RunList:
    def __init__(self):
        self.run_list = []

    def add(self, current):
        self.run_list.append(current)

    def summary(self):
        stop_pos = None
        avg_speed = -1
        if len(self.run_list) > 1:
            # get running time
            time = time_delta(
                self.run_list[0]["Date(UTC+0)"], self.run_list[-1]["Date(UTC+0)"]
            )
            if time == 0:
                time = 1

            # get moved distance
            dist = 0
            for i in range(1, len(self.run_list)):
                dist += geopy.distance.distance(
                    self.run_list[i]["Location(Lat,Lng)"],
                    self.run_list[i - 1]["Location(Lat,Lng)"],
                ).miles
            avg_speed = dist / time
            if avg_speed < settings.MOVE_SPEED:
                stop_pos = self.run_list[-1]

        return stop_pos, avg_speed


class IdleList:
    def __init__(self):
        self.idle_list = []

    def add(self, current):
        self.idle_list.append(current)

    def summary(self):
        start_pos = None
        avg_speed = -1
        if len(self.idle_list) > 1:
            # get avg speed while in idle state
            time = time_delta(
                self.idle_list[0]["Date(UTC+0)"], self.idle_list[-1]["Date(UTC+0)"]
            )
            if time == 0:
                time = 1
            dist = 0
            for i in range(1, len(self.idle_list)):
                dist += geopy.distance.distance(
                    self.idle_list[i]["Location(Lat,Lng)"],
                    self.idle_list[i - 1]["Location(Lat,Lng)"],
                ).miles
            avg_speed = dist / time

            if avg_speed >= settings.MOVE_SPEED:
                start_pos = self.idle_list[-1]

        return start_pos, avg_speed

