import os, sys, signal
import tempfile
import signal
import time
from multiprocessing.sharedctypes import Value, Array
from multiprocessing import Process, Pipe
import stat
import logging
import logging.handlers

# import gamin
import gfx
from daemon.runner import DaemonRunner

# from cloghandler import ConcurrentRotatingFileHandler


DEBUG = True # Program in debug mode doesn't call

LOG_FILENAME = '/var/log/swfdaemon.log' # Filename of log-file if log is written into file.
                  # None if log should be written to stdout.
                  # '/dev/null' otherwise

LOG_LEVEL = logging.DEBUG # can be one of following:
                         # logging.DEBUG, logging.INFO,
                         # logging.WARN, logging.ERROR, logging.CRITICAL

MAX_RETRY_NUMBER = 3    # max number of retrying to convert one file

LOG_FILE_SIZE = 512 * 1024

LOG_FILE_NUMBER = 5 # Number of log files

VERBOSE_LEVEL = 5   # Verbose level of each swf conversion
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

LOG_FOR_EACH_FILE = False   #Create log for each converted file if True do not create otherwise

COLLECT_LOG_FOR_EACH_FILE = False # write information from log for each converted file into common log

TEMP_SWF_FILENAME = "doc_temp.swf" # name of temporary swf file

ERROR_STRING_MAX_LENGTH = 160

RENDERING_TIME_LIMIT = 120  # Time limit for whole document rendering

PAGE_RENDERING_TIME_LIMIT = 30  # Time limit for separate page rendering

FINAL_SWF_FILENAME = "doc.swf"     # name of resulting swf file

CONVERT_READY_FILENAME = 'convert.ready' # filename of file which shows that conversion was finished

#CHOWN_COMMAND_STRING = '' # comand for change owner of directory
CHOWN_COMMAND_STRING = 'chown -R vagrant:vagrant %s' # comand for change owner of directory

#AJAX_CS_STRING = "php /var/www/html/production/ajax/cs.php %s" # AJAX running command
#AJAX_CS_STRING = "curl http://www.pdffiller.com/ajax/cs.php?convert_id=%s" # AJAX running command
AJAX_CS_STRING = "" # AJAX running command

STACK_DIR_PATH = "/vagrant/converter/stack/" # Path to stack directory

PID_FILE_PATH = '/var/run/main_daemon.pid' # full filename of daemon pid file

PID_FILE_TIMEOUT = 5    # timeout of pid file open

DAEMON_STDIN_PATH = '/dev/stdin' # path of daemon for stdin

DAEMON_STDOUT_PATH = '/dev/stdout' # path of daemon for stdout

DAEMON_STDERR_PATH = '/dev/stderr' # path of daemon for stderr

CHMOD_MASK = (stat.S_IRUSR | stat.S_IXUSR | stat.S_IWUSR |
               stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
               stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)


gfx.setparameter("zoom", "96")

PARAMETER_SEQUENCE = [None, 'poly2bitmap', 'bitmap']

LOG_LEVELS = {'TRACE': 'debug', 'DEBUG': 'debug',
              'VERBOSE': 'info', 'NOTICE': 'warn',
              'WARN': 'warn',
              'WARNING': 'warn', 'ERROR': 'error',
              'Error:': 'error', 'FATAL': 'critical'}

FORMAT = "%(levelname)s (%(asctime)s):  %(levelname)s | %(message)s"

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


def redirect_std(file_name, descriptor, logger):
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

def redirect_stdout(file_name, logger):
    """
    redirect inner c stdout to file with `filename`
    """
    return redirect_std(file_name, 1, logger)

def redirect_stderr(file_name, logger):
    """
    redirect inner c stdout to file with `filename`
    """
    return redirect_std(file_name, 2, logger)

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

def pdf2swf(fname, sname, result, error_string, log_filename,
            owner_pipe_connection=None,
            consider_pbm_notice=None, force_parameter=[]):
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

        try:
            # redirect inner C stdout to swftools.log
            log_name = os.path.join(dir, '%s.log' % junk)
            redirect_stdout(log_name, script_logger)
            redirect_stderr(log_name + ".err", script_logger)
            log_offset = 0
            # Set level of swftools logging
            gfx.verbose(VERBOSE_LEVEL)

            doc = gfx.open("pdf", fname)

            if not os.path.exists(os.path.join(dir, CONVERT_STATUS_FILENAME)):
                file(os.path.join(dir, CONVERT_STATUS_FILENAME), 'w').write('')
                script_logger.info('convert status file %s was created',
                                   os.path.join(dir, CONVERT_STATUS_FILENAME))
            status = file(os.path.join(dir, CONVERT_STATUS_FILENAME), 'w+')
            writeStatus(status, "Progress: %d/%d" % (0, doc.pages))

            if doc.getInfo("encrypted") == "yes":
                error_string.value = "File is encrypted"
                script_logger.warn("File %s is encrypted", fname)
#                raise Exception("File is Encrypted. Password is needed")

            if doc.getInfo("oktocopy") == 'no':
                script_logger.warn("file %s is protected from copying", fname)
                error_string.value = "File is protected from copying"
                raise Exception("File is protected from copying")

            # uncomment this when directory for pages will be implemented
            make_dirs(dir, doc.pages)
            os.chmod(dir, CHMOD_MASK)

            # flag which shows that poly2bitmap is needed
            use_pbm = False
            force_param = force_parameter[0] if force_parameter else None

            retry_cntr = MAX_RETRY_NUMBER

            if force_param:
                script_logger.info("convertation using %s was forced", force_param)
            script_logger.info("start to convert file %s "
                               "%s considering pbm notice",
                               fname, "" if consider_pbm else "not")
            while not os.path.exists(os.path.join(dir, TEMP_SWF_FILENAME)):
                # if function was called with farce_parameter use first of passed
                if force_param:
                    doc.setparameter(force_param, "1")
                else:
                    doc.setparameter("poly2bitmap", "1" if use_pbm else "0")
                writeStatus(status, "Progress: %d/%d" % (0, doc.pages))

                swf = gfx.SWF()
                swf.setparameter("flashversion", "9")
                pbm_was_found = False
                for page_number in xrange(1, doc.pages + 1):
                    owner_pipe_connection.send(time.time())
                    page = doc.getPage(page_number)
                    script_logger.debug("Try to convert page %d/%d",
                                        page_number, doc.pages)
                    swf.startpage(page.width, page.height)
                    if force_param:
                        script_logger.debug("Try to render page using %s",
                                            force_param)
                    else:
                        script_logger.debug("Try to render page %s pbm",
                                           "using" if use_pbm else "not using")
                    page.render(swf)
                    script_logger.debug("Finish to  render  ")
                    swf.endpage()
                    writeStatus(status,
                                "Progress: %d/%d" % (page_number, doc.pages))
                    if consider_pbm and not use_pbm and not force_param:
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
                err_string = ("There were %d retrying of %s "
                              "conversion but all them failed" % (MAX_RETRY_NUMBER, fname))
                error_string.value =  err_string[:ERROR_STRING_MAX_LENGTH]

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
            if not os.path.exists(os.path.join(dir, FINAL_SWF_FILENAME)):
                script_logger.warn("File %s was not created before 'Finished' status",
                                   FINAL_SWF_FILENAME)
            result.value = 1

            #script_logger.debug("Run command %s", CHOWN_COMMAND_STRING % dir)
            #if 1:
            #    os.system(CHOWN_COMMAND_STRING % dir)

            writeStatus(status, "Finished")

            #script_logger.debug("Run command %s", AJAX_CS_STRING % project_id)
            #if not DEBUG:
            #    os.system(AJAX_CS_STRING % project_id)
        except SIGTERM_Received, e:
            script_logger.error("Process was terminated")
            error_string.value = "Terminated by main process"
        except:
            script_logger.error("Error was occurred %s : %s",
                                str(sys.exc_info()[0]),
                                str(sys.exc_info()[1]))
            # do note write error status because file conversion may finish successfully with other parameters
            err_string = str(sys.exc_info()[0]) + ":" + str(sys.exc_info()[1])
            error_string.value = ("exception was raised %s" % err_string)[:ERROR_STRING_MAX_LENGTH]
            # writeStatus(status, "Error: " + str(sys.exc_info()[0]) + ":" + str(sys.exc_info()[1]))
    finally:
        if not owner_pipe_connection.closed:
            owner_pipe_connection.close()
        script_logger.debug("just before closing")
        if os.path.exists(sname):
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
        self.logger = logging.getLogger('swfdaemon')

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

    def start_convert_process(self, data, stack_file, consider_pbm, force_parameter):
        result = Value('b', 0)
        error_string = Array('c', ' ' * ERROR_STRING_MAX_LENGTH)

        rec_conn, send_conn = Pipe()

        fh, log_filename = tempfile.mkstemp(prefix="tmp_test_pdf_log_")
        args = (data, stack_file, result, error_string, log_filename)
        kwargs = {'consider_pbm_notice': consider_pbm,
                  'force_parameter': force_parameter,
                  'owner_pipe_connection': send_conn }
        self.logger.debug("run pdf2swf with args=%s kwargs=%s " , args, kwargs)
        p = Process(target=pdf2swf, args=args, kwargs=kwargs)
        p.start()
        p.document_rendering_start = time.time()
        p.page_rendering_start = None
        p.pipe_connection = rec_conn
        p.pipe_child_connection = send_conn
        p.log_filename = log_filename
        p.result = result
        p.error_string = error_string
        p.function_args = args
        p.function_kwargs = kwargs.copy()
        p.function_kwargs['force_parameter'] = kwargs['force_parameter'][1:]
        p.log_file_handler = fh
        os.close(p.log_file_handler)
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
            self.logger.warn('IOError skipping')
        else:
            data = file_pointer.read().strip()
            self.logger.debug("data is %s" , data)
            file_pointer.close()

            parsed_data = data.strip().rsplit(' ', 1)

            consider_pbm = None
            if len(parsed_data) > 1 and (parsed_data[1] in ('0', '1')):
               consider_pbm = bool(int(parsed_data[1]))
               data = parsed_data[0]
            self.start_convert_process(data, "%s%s" % (self.stack, path),
                                       consider_pbm, PARAMETER_SEQUENCE)

            self.logger.debug("New file was observed %s %s " , path, event)

    def init(self):
        """
        initialization method which inits WatchMonitor for monitoring directory
        and binds signal to terminate daemon
        """
        self.logger = logging.getLogger('swfdaemon')
        self.logfile = os.path.abspath(LOG_FILENAME)
        fh_ = logging.handlers.TimedRotatingFileHandler(self.logfile,
                                                        when='midnight',
                                                        interval=1)
        fh_.setFormatter(logging.Formatter(FORMAT))
        self.logger.addHandler(fh_)
        self.logger.setLevel(LOG_LEVEL)

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

    def _fail_to_render(self, process_obj, error_string):
        dir, junk = os.path.split(process_obj.function_args[0])
        if not os.path.exists(dir):
            return
        status = file(os.path.join(dir, CONVERT_STATUS_FILENAME), 'w+')
        writeStatus(status, "Error: %s" % error_string)

    def _copy_process_log(self, process_obj):
        if not os.path.exists(process_obj.log_filename):
            self.logger.warn('log file for %s has been copied and deleted', process_obj.function_args[0])
            return
        self.logger.debug('Trying to add %s log (%db) to main log',
                          process_obj.log_filename,
                          os.path.getsize(process_obj.log_filename))
        file(self.logfile, 'a').write(file(process_obj.log_filename, "r").read())
        self.logger.debug('log %s has been added', process_obj.log_filename)
        os.remove(process_obj.log_filename)

    def _close_pipe(self, proc):
        if not proc.pipe_child_connection.closed:
            proc.pipe_child_connection.close()
        if not proc.pipe_connection.closed:
            proc.pipe_connection.close()

    def terminate_process(self, proc, status):
        '''terminate process `proc` setting status `status` '''
        proc.error_string.value = status
        if not proc.pid:
            return
        self._close_pipe(proc)
        os.kill(proc.pid, signal.SIGKILL)
        proc._was_terminated = True
        self._copy_process_log(proc)
        self._fail_to_render(proc, status)
        if os.path.exists(proc.function_args[1]):
            os.remove(proc.function_args[1])
        self.processes.remove(proc)

    def clean_processes(self):
        """
        remove all processes which finished their tasks
        """
        to_remove = []
        for i in self.processes:
            current_time = time.time()
            if not i.is_alive():
                i.join()
                try:
                    self._copy_process_log(i)
                except Exception, e:
                    self.logger.error("Error was occurred %s : %s",
                                        str(sys.exc_info()[0]),
                                        str(sys.exc_info()[1]))
                if (not i.result.value):
                    force_parameter = i.function_kwargs.get('force_parameter', [])
                    if force_parameter:
                        args = i.function_args
                        kwargs = i.function_kwargs
                        self.start_convert_process(args[0], args[1],
                                  kwargs['consider_pbm_notice'], force_parameter)
                    else:
                        self.logger.error("Error was occurred while rendering %s",
                                     str(i.function_args[0]))
                        self._fail_to_render(i, i.error_string.value)
                        if os.path.exists(i.function_args[1]):
                            os.remove(i.function_args[1])
                self._close_pipe(i)
                self.processes.remove(i)
                continue
            if getattr(i, '_was_terminated', False):
                continue

            # read information about page rendering start time if it is in the pipe
            while i.pipe_connection.poll():
#                print "try to get data from pipe"
                i.page_rendering_start = i.pipe_connection.recv()
#                print "got data from pipe"

            if (current_time - i.document_rendering_start > RENDERING_TIME_LIMIT):
                self.terminate_process(i, 'Document rendering time exceeded')
            elif (i.page_rendering_start and
                  (current_time - i.page_rendering_start > PAGE_RENDERING_TIME_LIMIT)):
                self.terminate_process(i, 'Page rendering time exceeded')

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
        except SIGTERM_Received, e:
            self.logger.debug("Termination signal was received")
            for i in self.processes:
                self.terminate_process(i, 'Terminated')
            return
        except Exception, e:
            import traceback
            traceback.print_stack()

    def terminate(self):
       self.clean_processes()


if __name__ == "__main__":
    daemon_runner = DaemonRunner(StackListener())
    daemon_runner.parse_args()
    daemon_runner.do_action()
