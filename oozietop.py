#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import curses
import threading
import Queue
import signal
import logging as LOG
import time
import sys
import urllib
import json
from time import gmtime, strftime
import urllib2_kerberos
import urllib2

LOG.basicConfig(filename="oozietop.log", level=LOG.DEBUG)

resized_sig = False

q_stats = Queue.Queue()

p_wakeup = threading.Condition()


def wakeup_poller():
    p_wakeup.acquire()
    p_wakeup.notifyAll()
    p_wakeup.release()


class BaseUI(object):
    def __init__(self, win):
        self.win = win
        global mainwin
        self.maxy, self.maxx = mainwin.getmaxyx()
        self.resize(self.maxy, self.maxx)

    def resize(self, maxy, maxx):
        #LOG.debug("resize called y %d x %d" % (maxy, maxx))
        self.maxy = maxy
        self.maxx = maxx

    def addstr(self, y, x, line, flags=0):
        #LOG.debug("addstr with maxx %d" % self.maxx)
        self.win.addstr(y, x, line[:self.maxx-1], flags)
        self.win.clrtoeol()
        self.win.noutrefresh()


class SummaryUI(BaseUI):
    def __init__(self, height, width, server_count):
        BaseUI.__init__(self, curses.newwin(50, width, 0, 0))

    def update(self):
        self.win.erase()


class Main(object):
    def __init__(self, oozie_server):
        self.oozie_server = oozie_server

    def show_ui(self, stdscr):
        global mainwin
        mainwin = stdscr
        curses.use_default_colors()
        # w/o this for some reason takes 1 cycle to draw wins
        stdscr.refresh()
        signal.signal(signal.SIGWINCH, sigwinch_handler)
        TIMEOUT = 250
        stdscr.timeout(TIMEOUT)

        #server_count = len(self.servers)
        maxy, maxx = stdscr.getmaxyx()
        ui = SummaryUI(maxy, maxx, 5)

        LOG.debug("starting main loop")

        global resized_sig
        flash = None
        while True:
            ui.win.erase()
            row_count = 0
            ui.addstr(0, 0, strftime("%a, %d %b %Y %H:%M:%S +0500", gmtime()), curses.A_REVERSE)
            ui.addstr(1, 0, "%-40s %-20s %-10s %-40s %-40s" % ("JOB ID", "NAME", "STATUS", "START TIME", "END TIME"), curses.A_REVERSE)
            if resized_sig:
                resized_sig = False
                self.resize(ui)

            try:
                LOG.debug("Attempting poll of " + oozie_server.uri)
                workflows = oozie_server.poll()
                for workflow in workflows:
                    ui.addstr(row_count + 2, 0, "%-40s %-20s %-15s %-20s %-20s" %
                    (workflow[0], workflow[1], workflow[2], workflow[3], workflow[4]))
                    row_count += 1
            except Exception as e:
                LOG.error(e)

            ui.update()
            stdscr.clrtoeol()
            curses.doupdate()
            time.sleep(5)

    def resize(self, ui):
            curses.endwin()
            curses.doupdate()

            global mainwin
            mainwin.refresh()
            maxy, maxx = mainwin.getmaxyx()
            try:
                ui.resize(maxy, maxx)
                ui.addstr(0, 0, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime()), curses.A_REVERSE)
                ui.addstr(1, 0, "%-40s %-20s %-10s %-40s %-40s" % ("JOB ID", "NAME", "STATUS", "START TIME", "END TIME"), curses.A_REVERSE)
            except Exception as e:
                LOG.error("error at resize")


class OozieConnection(object):

    def __init__(self, host, port, kinit_truth):

        #get current timezone - This assumes Oozie has been setup for the system timezone
        self.tz = strftime("%Z", gmtime())
        self.host = host
        self.port = port
        self.kinit_truth = kinit_truth
        self.uri = "http://" + host + ":" + port + "/oozie/v1/jobs?jobType=wf&timezone=%s" % self.tz
        LOG.debug(self.uri)
        self.failed_count = 0
        self.suspended_count = 0
        self.killed_count = 0
        self.succeeded_count = 0
        self.prep_count = 0
        self.running_count = 0
        self.workflows = []

    def poll(self):
        #If Kerberos, use urllib2/urllib2_kerberos, if not, use urllib
        if self.kinit_truth == "true":
            try:
                opener = urllib2.build_opener()
                opener.add_handler(urllib2_kerberos.HTTPKerberosAuthHandler())
                resp = opener.open(self.uri)
                json_response = resp.read()

                #Create a JSON object
            except:
                LOG.error("Error connecting to the Oozie server with kerberos")
            #Create a JSON object
            try:
                json_object = json.loads(json_response)
            except:
                LOG.error("Error parsing the JSON from Oozie")
        else:
            try:
                raw_json = urllib.urlopen(self.uri)
            except Exception as e:
                LOG.error("Error connecting to the Oozie server " + e)

            #Create a JSON object
            try:
                json_object = json.load(raw_json)
            except:
                LOG.error("Error parsing the JSON from Oozie")
        self.workflows = []
        #iterate through the json and pull out the workflows
        for job in json_object[u'workflows']:
            row = [job[u'id'], job[u'appName'], job[u'status'], job[u'startTime'], job[u'endTime']]
            self.workflows.append(row)
        #iterate through the workflows and get status
        for workflow in self.workflows:
            if workflow[2] == "FAILED":
                self.failed_count += 1

            elif workflow[2] == "SUSPENDED":
                self.suspended_count += 1

            elif workflow[2] == "KILLED":
                self.killed_count += 1

            elif workflow[2] == "SUCCEEDED":
                self.succeeded_count += 1

            elif workflow[2] == "PREP":
                self.prep_count += 1

            elif workflow[2] == "RUNNING":
                self.running_count += 1
        #LOG.debug(self.workflows)
        return self.workflows


def sigwinch_handler(*nada):
    LOG.debug("sigwinch called")
    global resized_sig
    resized_sig = True

if __name__ == '__main__':
    LOG.debug("startup")
    if len(sys.argv) < 3:
        #LOG.ERROR("Missing Hostname and port")
        print "Missing hostname and port"
        sys.exit(2)
    oozie_server = OozieConnection(sys.argv[1], sys.argv[2], False)

    ui = Main(oozie_server)
    curses.wrapper(ui.show_ui)