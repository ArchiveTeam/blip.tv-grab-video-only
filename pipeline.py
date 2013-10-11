import datetime
from distutils.version import StrictVersion
import fcntl
import os
import pty
from seesaw import item
import seesaw
from seesaw.config import NumberConfigValue
from seesaw.externalprocess import WgetDownload
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.task import SimpleTask, LimitConcurrent, ConditionalTask
from seesaw.tracker import (GetItemFromTracker, SendDoneToTracker,
    PrepareStatsForTracker, UploadWithTracker)
from seesaw.util import find_executable
import shutil
import subprocess
import time
from tornado.ioloop import IOLoop, PeriodicCallback


# check the seesaw version before importing any other components
if StrictVersion(seesaw.__version__) < StrictVersion("0.0.15"):
    raise Exception("This pipeline needs seesaw version 0.0.15 or higher.")


# # Begin AsyncPopen fix
class AsyncPopenFixed(seesaw.externalprocess.AsyncPopen):
    """
    Start the wait_callback after setting self.pipe, to prevent an infinite
    spew of "AttributeError: 'AsyncPopen' object has no attribute 'pipe'"
    """
    def run(self):
        self.ioloop = IOLoop.instance()
        (master_fd, slave_fd) = pty.openpty()

        # make stdout, stderr non-blocking
        fcntl.fcntl(master_fd, fcntl.F_SETFL,
            fcntl.fcntl(master_fd, fcntl.F_GETFL) | os.O_NONBLOCK)

        self.master_fd = master_fd
        self.master = os.fdopen(master_fd)

        # listen to stdout, stderr
        self.ioloop.add_handler(master_fd, self._handle_subprocess_stdout,
            self.ioloop.READ)

        slave = os.fdopen(slave_fd)
        self.kwargs["stdout"] = slave
        self.kwargs["stderr"] = slave
        self.kwargs["close_fds"] = True
        self.pipe = subprocess.Popen(*self.args, **self.kwargs)

        self.stdin = self.pipe.stdin

        # check for process exit
        self.wait_callback = PeriodicCallback(self._wait_for_end, 250)
        self.wait_callback.start()

seesaw.externalprocess.AsyncPopen = AsyncPopenFixed
# # End AsyncPopen fix


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_LUA will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string
WGET_LUA = find_executable(
    "Wget+Lua",
    ["GNU Wget 1.14.lua.20130523-9a5c"],
    [
        "./wget-lua",
        "./wget-lua-warrior",
        "./wget-lua-local",
        "../wget-lua",
        "../../wget-lua",
        "/home/warrior/wget-lua",
        "/usr/bin/wget-lua"
    ]
)

if not WGET_LUA:
    raise Exception("No usable Wget+Lua found.")


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = "20131011.01"
USER_AGENT = "Something"
TRACKER_ID = 'bloopertv'
TRACKER_HOST = 'tracker.archiveteam.org'


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class PrepareDirectories(SimpleTask):
    def __init__(self, file_prefix):
        SimpleTask.__init__(self, "PrepareDirectories")
        self.file_prefix = file_prefix

    def process(self, item):
        item_name = item["item_name"]

        # I expect a full URL here
        assert '-' in item_name
        assert '.' in item_name
        assert '/' in item_name
        assert 'http://' in item_name
        file_id = item_name.split('/')[-1]  # quick portable way to get filename

        dirname = "/".join((item["data_dir"], file_id))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item["item_dir"] = dirname

        item["file_id"] = file_id
        item["file_base"] = "%s-%s-%s" % (self.file_prefix,
            time.strftime("%Y%m%d-%H%M%S"),
            file_id,)

        open("%(item_dir)s/%(file_base)s" % item, "w").close()


class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        os.rename("%(item_dir)s/%(file_base)s" % item,
              "%(data_dir)s/%(file_base)s" % item)

        shutil.rmtree("%(item_dir)s" % item)


###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title="Blip.tv",
    project_html="""
    <img class="project-logo" alt="" src="http://archiveteam.org/images/5/5f/Blip_web_logo.png" height="50"/>
    <h2>Blip.tv <span class="links"><a href="http://blip.tv/">Website</a> &middot; <a href="http://%s/%s/">Leaderboard</a></span></h2>
    <p>Archiving videos from <b>Blip.tv</b></p>
    """ % (TRACKER_HOST, TRACKER_ID)
    , utc_deadline=datetime.datetime(2013, 11, 07, 00, 00, 1)
)

pipeline = Pipeline(
    GetItemFromTracker("http://%s/%s" % (TRACKER_HOST, TRACKER_ID), downloader,
        VERSION),
    PrepareDirectories(file_prefix="bliptv"),
    WgetDownload([
        WGET_LUA,
        "-U", USER_AGENT,
        # "-nv",
        "-o", ItemInterpolation("%(item_dir)s/wget.log"),
        "--no-check-certificate",
        "--truncate-output",
        "--output-document", ItemInterpolation("%(item_dir)s/%(file_base)s"),
        "-e", "robots=off",
        "--rotate-dns",
        "--timeout", "60",
        "--level=inf",
        "--tries", "20",
        "--waitretry", "5",
        ItemInterpolation("%(item_name)s")
        ],
        max_tries=5,
        accept_on_exit_code=[ 0 ],
    ),
    PrepareStatsForTracker(
        defaults={ "downloader": downloader, "version": VERSION },
        file_groups={
            "data": [ ItemInterpolation("%(item_dir)s/%(file_base)s") ]
            }
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=4, default="1",
        name="shared:rsync_threads", title="Rsync threads",
        description="The maximum number of concurrent uploads."),
        UploadWithTracker(
            "http://tracker.archiveteam.org/%s" % TRACKER_ID,
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation("%(data_dir)s/%(file_base)s")
                ],
            rsync_target_source_path=ItemInterpolation("%(data_dir)s/"),
            rsync_extra_args=[
                "--recursive",
                "--partial",
                "--partial-dir", ".rsync-tmp"
            ]
            ),
    ),
    SendDoneToTracker(
        tracker_url="http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue("stats")
    )
)
