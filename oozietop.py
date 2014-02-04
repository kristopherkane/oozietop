
from optparse import OptionParser

import curses
import threading, Queue
import socket
import signal
import re, StringIO
import logging as LOG
import time


LOG.disable(LOG.CRITICAL)

resized_sig = False


class BaseUI(object):
    def __init__(self, win):
        self.win = win
        global mainwin
        self.maxy, self.maxx = mainwin.getmaxyx()
        self.resize(self.maxy, self.maxx)

    def resize(self, maxy, maxx):
        LOG.debug("resize called y %d x %d" % (maxy, maxx))
        self.maxy = maxy
        self.maxx = maxx

    def addstr(self, y, x, line, flags = 0):
        LOG.debug("addstr with maxx %d" % (self.maxx))
        self.win.addstr(y, x, line[:self.maxx-1], flags)
        self.win.clrtoeol()
        self.win.noutrefresh()


class SummaryUI(BaseUI):
    def __init__(self, height, width, server_count):
        BaseUI.__init__(self, curses.newwin(1, width, 0, 0))

    def update(self):
        self.win.erase()
        self.addstr(0, 0, "This is just a test")

class Main(object):
    def __init__(self):
        pass

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
        """# start the polling threads
        pollers = [StatPoller(server) for server in self.servers]
        for poller in pollers:
            poller.setName("PollerThread:" + server)
            poller.setDaemon(True)
            poller.start()  """

        LOG.debug("starting main loop")
        global resized_sig
        flash = None


        while True:
            if resized_sig:
                resized_sig = False
                self.resize(ui)
            time.sleep(5)
            ui.update()
            stdscr.clrtoeol()
            curses.doupdate()

    def resize(self, ui):
            curses.endwin()
            curses.doupdate()

            global mainwin
            mainwin.refresh()
            maxy, maxx = mainwin.getmaxyx()
            ui.resize(maxy, maxx)

def sigwinch_handler(*nada):
    LOG.debug("sigwinch called")
    global resized_sig
    resized_sig = True

if __name__ == '__main__':
    LOG.debug("startup")

    ui = Main()
    curses.wrapper(ui.show_ui)