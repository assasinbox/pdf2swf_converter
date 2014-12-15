import os, sys
import tempfile
import signal
import time
from multiprocessing.sharedctypes import Value
from multiprocessing import Process
import stat
import logging
import logging.handlers

import gamin
import gfx
from daemon.runner import DaemonRunner

from cloghandler import ConcurrentRotatingFileHandler



LOG_FILENAME = '/home/developer/sandbox/pdf.log' # Filename of log-file if log is written into file.
                  # None if log should be written to stdout.
                  # '/dev/null' otherwise

LOG_LEVEL = logging.INFO # can be one of following:
                         # logging.DEBUG, logging.INFO,
                         # logging.WARN, logging.ERROR, logging.CRITICAL

MAX_RETRY_NUMBER = 10    # max number of retrying to convert one file

LOG_FILE_SIZE = 512 * 1024

LOG_FILE_NUMBER = 5 # Number of log files

VERBOSE_LEVEL = 3   # Verbose level of each swf conversion
                  # level=-1          Log nothing
                  # level=0 (fatal)   Log only fatal errors
                  # level=1 (error)   Log only fatal errors and errors
                  # level=2 (warn)    Log all errors and warnings
                  # level=3 (notice)  Log also some rudimentary data about the parsing/conversion
                  # level=4 (verbose) Log some additional parsing information
                  # level=5 (debug)   Log debug statements
                  # level=6 (trace)   Log extended debug statements

CONSIDER_PBM_NOTICE = False  # whether pbm should be cosidered (True) or not (False)

CONVERT_STATUS_FILENAME = 'convert.status' # name of file with status of conversion

LOG_FOR_EACH_FILE = True   #Create log for each converted file if True do not create otherwise

COLLECT_LOG_FOR_EACH_FILE = False # write information from log for each converted file into common log

TEMP_SWF_FILENAME = "doc_temp.swf" # name of temporary swf file

FINAL_SWF_FILENAME = "doc.swf"     # name of resulting swf file

CONVERT_READY_FILENAME = 'convert.ready' # filename of file which shows that conversion was finished

CHOWN_COMMAND_STRING = 'chown -R www-data:www-data %s' # comand for change owner of directory

AJAX_CS_STRING = "php /var/www/html/production/ajax/cs.php %s" # AJAX running command

STACK_DIR_PATH = "/var/pdffiller/pdffiller-stack/" # Path to stack directory

PID_FILE_PATH = '/tmp/test_daemon.pid' # full filename of daemon pid file

PID_FILE_TIMEOUT = 5    # timeout of pid file open

DAEMON_STDIN_PATH = '/dev/stdin' # path of daemon for stdin

DAEMON_STDOUT_PATH = '/dev/stdout' # path of daemon for stdout

DAEMON_STDERR_PATH = '/dev/stderr' # path of daemon for stderr

CHMOD_MASK = (stat.S_IRUSR | stat.S_IXUSR | stat.S_IWUSR |
               stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
               stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)


logger = logging.getLogger('swfdaemon')
FORMAT = "%(levelname)s (%(asctime)s):  %(levelname)s | %(message)s"

gfx.setparameter("zoom", "96")

LOG_LEVELS = {'TRACE': 'debug', 'DEBUG': 'debug',
              'VERBOSE': 'info', 'NOTICE': 'warn',
              'WARN': 'warn',
              'WARNING': 'warn', 'ERROR': 'error',
              'Error:': 'error', 'FATAL': 'critical'}

def make_dirs(base_dir, pages):
    """
    create subdirectories in directory `basedir` which amount is passed as `pages` param
    """
    base_dir += "/"
    for i in range(0, pages):
        newdir = base_dir + str(i)
        if not os.path.isdir(newdir):
            os.mkdir(newdir)
        os.chmod(newdir, CHMOD_MASK)


def writeStatus(file, status):
    """
    write status into file
    """
    file.truncate(0)
    file.seek(0)
    file.write(status + "\n")
    file.flush()


def redirect_std(file_name, descriptor):
    logger.debug("Redirecting stdout")
    sys.stdout.flush() # <--- important when redirecting to files
    newstdout = os.dup(descriptor)
    full_name = file_name
    file(full_name, 'w').close()
    file_for_stdout = os.open(full_name, os.O_WRONLY)
    os.dup2(file_for_stdout, descriptor)
    os.close(file_for_stdout)
    sys.stdout = os.fdopen(newstdout, 'w')
    return newstdout

def redirect_stdout(file_name):
    """
    redirect inner c stdout to file with `filename`
    """
    return redirect_std(file_name, 1)

def redirect_stderr(file_name):
    """
    redirect inner c stdout to file with `filename`
    """
    return redirect_std(file_name, 2)

def log_swftools_msgs(swf_tools_msg, logger, log_file=''):
    """
    function which write swftools log messages to python log
    """
    level, msg = swf_tools_msg.split(' ', 1)
    getattr(logger, LOG_LEVELS.get(level, 'info'))(log_file + msg.strip())

def log_swftools_conversion(fname, logger, log_name):
    """ write swftools log into python logging"""
    if not COLLECT_LOG_FOR_EACH_FILE:
        return
    for log_line in file(log_name, "r").readlines():
        log_swftools_msgs(log_line, logger, fname)
    for log_line in file(log_name + ".err", "r").readlines():
        log_swftools_msgs(log_line, logger, fname)

def pdf2swf(fname, sname, result, log_filename,
            consider_pbm_notice=None, force_pbm=False):
    """
    convert function
    """
    consider_pbm = consider_pbm_notice
    if consider_pbm is None:
        consider_pbm = CONSIDER_PBM_NOTICE

    script_logger = logging.getLogger('swfdaemon_%s' % str(log_filename))
    fh = logging.FileHandler(log_filename)
    fh.setFormatter(logging.Formatter(FORMAT))
    script_logger.addHandler(fh)
    script_logger.setLevel(LOG_LEVEL)

    log_name = ''
    try:
        dir, junk = os.path.split(fname)
        project_id = os.path.basename(dir)

        if not os.path.exists(os.path.join(dir, CONVERT_STATUS_FILENAME)):
            file(os.path.join(dir, CONVERT_STATUS_FILENAME), 'w').write('')
            script_logger.info('convert status file %s was created',
                               os.path.join(dir, CONVERT_STATUS_FILENAME))
        status = file(os.path.join(dir, CONVERT_STATUS_FILENAME), 'w+')

        # redirect inner C stdout to swftools.log
        log_name = os.path.join(dir, '%s.log' % junk)
        redirect_stdout(log_name)
        redirect_stderr(log_name + ".err")
        log_offset = 0
        # Set level of swftools logging
        gfx.verbose(VERBOSE_LEVEL)

        try:
            doc = gfx.open("pdf", fname)

            if doc.getInfo("encrypted") == "yes":
                script_logger.warn("File %s is encrypted", fname)
#                raise Exception("File is Encrypted. Password is needed")

            if doc.getInfo("oktocopy") == 'no':
                script_logger.warn("file %s is protected from copying", fname)
                raise Exception("File is protected from copying")

            # uncomment this when directory for pages will be implemented
            make_dirs(dir, doc.pages)
            os.chmod(dir, CHMOD_MASK)

            # flag which shows that poly2bitmap is needed
            use_pbm = False or force_pbm

            retry_cntr = MAX_RETRY_NUMBER

            if force_pbm:
                script_logger.info("convertation using poly2bitmap was forced")
            script_logger.info("start to convert file %s "
                               "%s considering pbm notice",
                               fname, "" if consider_pbm else "not")
            while not os.path.exists(os.path.join(dir, TEMP_SWF_FILENAME)):
                doc.setparameter("poly2bitmap", "1" if use_pbm else "0")
                writeStatus(status, "Progress: %d/%d" % (0, doc.pages))

                swf = gfx.SWF()
                swf.setparameter("flashversion", "9");
                pbm_was_found = False
                for page_number in xrange(1, doc.pages + 1):
                    page = doc.getPage(page_number)
                    script_logger.debug("Try to convert page %d/%d", page_number, doc.pages)
                    swf.startpage(page.width, page.height)
                    script_logger.debug("Try to render page %s pbm",
                                       "using" if use_pbm else "not using")
                    page.render(swf)
                    script_logger.debug("Finish to  render  ")
                    swf.endpage()
                    writeStatus(status,
                                "Progress: %d/%d" % (page_number, doc.pages))
                    if consider_pbm and not use_pbm:
                        script_logger.debug("checking for pbm images "
                                            "(starting from %s)",
                                            str(log_offset))
                        swf_log_file = file(log_name, "r")
                        swf_log_file.seek(log_offset)
                        data = swf_log_file.read()
                        log_offset += len(data)
                        pbm_was_found = data.count('pbm pictures')
                        script_logger.debug("was checked until %d", log_offset)
                        if pbm_was_found:
                            script_logger.warn("bpm image was found. "
                                               "File %s will be converted "
                                               "using poly2bitmap", fname)
                            use_pbm = True
                            break
                else:
                    script_logger.debug("Try to save file %s",
                                        os.path.join(dir, TEMP_SWF_FILENAME))
                    swf.save(os.path.join(dir, TEMP_SWF_FILENAME))

                log_swftools_conversion(fname, script_logger, log_name)
                if not (os.path.exists(os.path.join(dir, TEMP_SWF_FILENAME))
                        or pbm_was_found):
                    script_logger.warn("Conversion of %s failed. "
                                       "Try to convert one more time", fname)
                    retry_cntr -= 1
                    if not retry_cntr:
                        break

            if not retry_cntr:
                script_logger.error("There were %d retrying of %s "
                                    "conversion but all them failed",
                                    MAX_RETRY_NUMBER, fname)
                return
            script_logger.debug("%s was saved",
                                os.path.join(dir, TEMP_SWF_FILENAME))

            script_logger.info("Conversion of %s succeeded", fname)
            os.rename(os.path.join(dir, TEMP_SWF_FILENAME),
                      os.path.join(dir, FINAL_SWF_FILENAME))

            open(os.path.join(dir, CONVERT_READY_FILENAME), 'w').close() # FIXME: for backward support
            writeStatus(status, "Finished")
            result.value = 1
            os.system(CHOWN_COMMAND_STRING % dir)
            os.system(AJAX_CS_STRING % project_id)
        except:
            script_logger.error("Error was occurred %s : %s",
                                str(sys.exc_info()[0]),
                                str(sys.exc_info()[1]))
            writeStatus(status, "Error: " + str(sys.exc_info()[0]) + ":" + str(sys.exc_info()[1]))
    finally:
        script_logger.debug("just before closing")
        os.remove(sname)
        if os.path.exists(log_name) and not LOG_FOR_EACH_FILE:
            os.remove(log_name)
    return result

class SIGTERM_Received(Exception):
    pass

class StackListener(object):
    """
    class which listens directory and runs convert function when it is needed
    """
    stack = STACK_DIR_PATH
    def __init__(self):
        self.processes = []
        self.pidfile_path = PID_FILE_PATH
        self.pidfile_timeout = PID_FILE_TIMEOUT
        self.stdin_path = DAEMON_STDIN_PATH
        self.stdout_path = DAEMON_STDOUT_PATH
        self.stderr_path = DAEMON_STDERR_PATH

    def run(self):
        """
        method which is used by DaemonRunner as main loop
        """
        self.init()
        self.main_loop()
        self.terminate()

    def start_convert_process(self, data, stack_file, consider_pbm, force_pbm):
        result = Value('b', 0)
        fh, log_filename = tempfile.mkstemp(prefix="tmp_pdf_log_")
        args = (data, stack_file, result, log_filename)
        kwargs = {'consider_pbm_notice': consider_pbm,
                'force_pbm': force_pbm}
        p = Process(target=pdf2swf, args=args, kwargs=kwargs)
        p.start()
        p.log_filename = log_filename
        p.result = result
        p.function_args = args
        p.function_kwargs = kwargs
        self.processes.append(p)

    def callback(self, path, event):
        """
        callback method which is used by WatchMonitor to convert new files
        """
        if event not in (gamin.GAMChanged, gamin.GAMExists):
            return
        try:
            if os.path.isdir(path):
                return
            file_pointer = open("%s%s" % (self.stack, path))
        except IOError:
            logger.warn('IOError skipping')
        else:
            data = file_pointer.read().strip()
            logger.debug("data is %s" , data)
            file_pointer.close()

            parsed_data = data.strip().rsplit(' ', 1)

            consider_pbm = None
            if len(parsed_data) > 1 and (parsed_data[1] in ('0', '1')):
               consider_pbm = bool(int(parsed_data[1]))
               data = parsed_data[0]

            self.start_convert_process(data, "%s%s" % (self.stack, path),
                                       consider_pbm, False)

            logger.debug("File %s %s " , path, event)

    def init(self):
        """
        initialization method which inits WatchMonitor for monitoring directory
        and binds signal to terminate daemon
        """
        logger = logging.getLogger('swfdaemon')
        self.logfile = os.path.abspath(LOG_FILENAME)
        fh_ = logging.handlers.TimedRotatingFileHandler(self.logfile,
                                                        when='midnight',
                                                        interval=1)
        fh_.setFormatter(logging.Formatter(FORMAT))
        logger.addHandler(fh_)
        logger.setLevel(LOG_LEVEL)

        self.mon = gamin.WatchMonitor()
        self.mon.watch_directory(self.stack, self.callback)
        def raise_exc(*args):
            raise SIGTERM_Received()
        signal.signal (signal.SIGTERM, raise_exc)
        # addition of the sleep timeout is needed because due to the
        # round trip between the client and the gam_server
        # events may not be immediately available after
        # the monitor creation to the client
        time.sleep(1)

    def clean_processes(self):
        """
        remove all processes which finished their tasks
        """
        for i in self.processes:
            if not i.is_alive():
                i.join()
                self.processes.remove(i)
                try:
                    file(self.logfile, 'a').write(file(i.log_filename, "r").read())
                    os.remove(i.log_filename)
                except Exception, e:
                    logger.error("Error was occurred %s : %s",
                                        str(sys.exc_info()[0]),
                                        str(sys.exc_info()[1]))
                if (not i.result.value):
                    if (not i.function_kwargs.get('force_pbm', 0)):
                        args = i.function_args
                        kwargs = i.function_kwargs
                        self.start_convert_process(args[0], args[1],
                                           kwargs['consider_pbm_notice'], True)
                    else:
                        dir, junk = os.path.split(i.function_args[0])
                        status = file(os.path.join(dir, CONVERT_STATUS_FILENAME), 'w+')
                        writeStatus(status, "Error")

    def main_loop(self):
        """
        daemon main loop
        """
        try:
            while True:
                if self.mon.event_pending():
                    # this check is neccessary to catch SIGTERM exception
                    self.mon.handle_one_event()  ##blocking call
                time.sleep(.1)
                self.clean_processes()
#                logger.debug("Number of active threads: %d",
#                                    len(self.processes))
        except SIGTERM_Received, e:
           logger.debug("Termination signal was received")
           return

    def terminate(self):
       self.clean_processes()


if __name__ == "__main__":
    daemon_runner = DaemonRunner(StackListener())
    daemon_runner.parse_args()
    daemon_runner.do_action()



