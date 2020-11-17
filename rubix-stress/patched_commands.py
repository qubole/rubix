"""
The commands module contains the base definition for
a generic Qubole command and the implementation of all
the specific commands
"""

from __future__ import print_function
from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.exception import ParseError
from qds_sdk.account import Account
from qds_sdk.util import GentleOptionParser, OptionParsingError, OptionParsingExit, _is_cloud_url
from optparse import SUPPRESS_HELP

import boto
import time
import logging
import sys
import re
import pipes
import os
import json
import signal

log = logging.getLogger("qds_commands")

# Pattern matcher for s3 path
_URI_RE = re.compile(r's3://([^/]+)/?(.*)')


class Command(Resource):
    """
    qds_sdk.Command is the base Qubole command class. Different types of Qubole
    commands can subclass this.
    """

    """all commands use the /commands endpoint"""
    rest_entity_path = "commands"

    listusage = "<subcommand> list [options]"
    listparser = GentleOptionParser(usage=listusage)
    listparser.add_option("-p", "--page", dest="page", type="int",
                          help="page number")
    listparser.add_option("-r", "--per-page", dest="per_page", type="int",
                          help="number of commands to be retrieved per page")
    listparser.add_option("-a", "--all-users", dest="all_users", type="int", default=0,
                          help="get the command history of all users. default: 0")
    listparser.add_option("-i", "--include-query-properties", action="store_true", dest="include_query_properties",
                          default=False, help="displays query properties such as tags and query history comments. "
                                              "default: False")
    listparser.add_option("-s", "--start-date", dest="start_date",
                          help="the date from which you want the command history")
    listparser.add_option("-e", "--end-date", dest="end_date",
                          help="the date until which you want the command history")

    @staticmethod
    def is_done(status):
        """
        Does the status represent a completed command
        Args:
            `status`: a status string

        Returns:
            True/False
        """
        return status == "cancelled" or status == "done" or status == "error"

    @staticmethod
    def is_success(status):
        return status == "done"

    @classmethod
    def list(cls, **kwargs):
        """
        List a command by issuing a GET request to the /command endpoint

        Args:
            `**kwargs`: Various parameters can be used to filter the commands such as:
                        * command_type - HiveQuery, PrestoQuery, etc. The types should be in title case.
                        * status - failed, success, etc
                        * name
                        * command_id
                        * qbol_user_id
                        * command_source
                        * page
                        * cluster_label
                        * session_id, etc

            For example - Command.list(command_type = "HiveQuery", status = "success")
        """
        conn = Qubole.agent()
        params = {}
        for k in kwargs:
            if kwargs[k]:
                params[k] = kwargs[k]
        params = None if not params else params
        return conn.get(cls.rest_entity_path, params=params)

    @classmethod
    def listparse(cls, args):
        try:
            (options, args) = cls.listparser.parse_args(args)
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.listparser.format_help())
        except OptionParsingExit as e:
            return None

        return vars(options)

    @classmethod
    def create(cls, **kwargs):
        """
        Create a command object by issuing a POST request to the /command endpoint
        Note - this does not wait for the command to complete

        Args:
            `**kwargs`: keyword arguments specific to command type

        Returns:
            Command object
        """

        conn = Qubole.agent()
        if kwargs.get('command_type') is None:
            kwargs['command_type'] = cls.__name__
        if kwargs.get('tags') is not None:
            kwargs['tags'] = kwargs['tags'].split(',')

        return cls(conn.post(cls.rest_entity_path, data=kwargs))

    @classmethod
    def run(cls, **kwargs):
        """
        Create a command object by issuing a POST request to the /command endpoint
        Waits until the command is complete. Repeatedly polls to check status

        Args:
            `**kwargs`: keyword arguments specific to command type

        Returns:
            Command object
        """

        # vars to keep track of actual logs bytes (err, tmp) and new bytes seen in each iteration
        err_pointer, tmp_pointer, new_bytes = 0, 0, 0
        print_logs_live = kwargs.pop("print_logs_live", None)  # We don't want to send this to the API.

        cmd = cls.create(**kwargs)

        while not Command.is_done(cmd.status):
            time.sleep(Qubole.poll_interval)
            cmd = cls.find(cmd.id)
            if print_logs_live is True:
                log, err_length, tmp_length = cmd.get_log_partial(err_pointer, tmp_pointer)

                # if err length is non zero, then tmp_pointer needs to be reset to the current tmp_length as the
                # err_length will contain the full set of logs from last seen non-zero err_length.
                if err_length != "0":
                    err_pointer += int(err_length)
                    new_bytes = int(err_length) + int(tmp_length) - tmp_pointer
                    tmp_pointer = int(tmp_length)
                else:
                    tmp_pointer += int(tmp_length)
                    new_bytes = int(tmp_length)

                if len(log) > 0 and new_bytes > 0:
                    print(log[-new_bytes:], file=sys.stderr)

        return cmd

    @classmethod
    def cancel_id(cls, id):
        """
        Cancels command denoted by this id

        Args:
            `id`: command id
        """
        conn = Qubole.agent()
        data = {"status": "kill"}
        return conn.put(cls.element_path(id), data)

    def cancel(self):
        """
        Cancels command represented by this object
        """
        self.__class__.cancel_id(self.id)

    @classmethod
    def get_log_id(cls, id):
        """
        Fetches log for the command represented by this id

        Args:
            `id`: command id
        """
        conn = Qubole.agent()
        r = conn.get_raw(cls.element_path(id) + "/logs")
        return r.text

    def get_log(self):
        """
        Fetches log for the command represented by this object

        Returns:
            The log as a string
        """
        log_path = self.meta_data['logs_resource']
        conn = Qubole.agent()
        r = conn.get_raw(log_path)
        return r.text

    def get_log_partial(self, err_pointer=0, tmp_pointer=0):
        """
        Fetches log (full or partial) for the command represented by this object
        Accepts:
            err_pointer(int): Pointer to err text bytes we've received so far, which will be passed to next api call
                to indicate pointer to fetch logs.
            tmp_pointer(int): Same as err_pointer except it indicates the bytes of tmp file processed.
        Returns:
            An array where the first field is actual log (string), while 2nd & 3rd are counts of err and tmp bytes
                which have been returned by api in addition to the given pointers.
        """
        log_path = self.meta_data['logs_resource']
        conn = Qubole.agent()
        r = conn.get_raw(log_path, params={'err_file_processed': err_pointer, 'tmp_file_processed': tmp_pointer})
        if 'err_length' in r.headers.keys() and 'tmp_length' in r.headers.keys():
            return [r.text, r.headers['err_length'], r.headers['tmp_length']]
        return [r.text, 0, 0]

    @classmethod
    def get_jobs_id(cls, id):
        """
        Fetches information about the hadoop jobs which were started by this
        command id. This information is only available for commands which have
        completed (i.e. Status = 'done', 'cancelled' or 'error'.) Also, the
        cluster which ran this command should be running for this information
        to be available. Otherwise only the URL and job_id is shown.

        Args:
            `id`: command id
        """
        conn = Qubole.agent()
        r = conn.get_raw(cls.element_path(id) + "/jobs")
        return r.text

    def get_results(self, fp=sys.stdout, inline=True, delim=None, fetch=True, qlog=None, arguments=[]):
        """
        Fetches the result for the command represented by this object

        get_results will retrieve results of the command and write to stdout by default.
        Optionally one can write to a filestream specified in `fp`. The `inline` argument
        decides whether the result can be returned as a CRLF separated string. In cases where
        the results are greater than 20MB, get_results will attempt to read from s3 and write
        to fp. The retrieval of results from s3 can be turned off by the `fetch` argument

        Args:
            `fp`: a file object to write the results to directly
            `inline`: whether or not results are returned inline as CRLF separated string
            `fetch`: True to fetch the result even if it is greater than 20MB, False to
                     only get the result location on s3
        """
        result_path = self.meta_data['results_resource']

        conn = Qubole.agent()

        include_header = "false"
        if len(arguments) == 1:
            include_header = arguments.pop(0)
            if include_header not in ('true', 'false'):
                raise ParseError("incude_header can be either true or false")

        r = conn.get(result_path, {'inline': inline, 'include_headers': include_header})
        if r.get('inline'):
            raw_results = r['results']
            encoded_results = raw_results.encode('utf8')
            if sys.version_info < (3, 0, 0):
                fp.write(encoded_results)
            else:
                import io
                if isinstance(fp, io.TextIOBase):
                    if hasattr(fp, 'buffer'):
                        fp.buffer.write(encoded_results)
                    else:
                        fp.write(raw_results)
                elif isinstance(fp, io.BufferedIOBase) or isinstance(fp, io.RawIOBase):
                    fp.write(encoded_results)
                else:
                    # Can this happen? Don't know what's the right thing to do in this case.
                    pass
        else:
            if fetch:
                if not boto.config.has_section('s3'):
                    boto.config.add_section('s3')
                boto.config.set('s3', 'use-sigv4', 'True')
                storage_credentials = conn.get(Account.credentials_rest_entity_path)
                host = storage_credentials['region_endpoint'] if storage_credentials[
                    'region_endpoint'] else "s3.amazonaws.com"
                boto_conn = boto.connect_s3(aws_access_key_id=storage_credentials['storage_access_key'],
                                            aws_secret_access_key=storage_credentials['storage_secret_key'],
                                            security_token=storage_credentials['session_token'],
                                            host=host)
                log.info("Starting download from result locations: [%s]" % ",".join(r['result_location']))
                # fetch latest value of num_result_dir
                num_result_dir = Command.find(self.id).num_result_dir

                # If column/header names are not able to fetch then use include header as true
                if include_header.lower() == "true" and qlog is not None:
                    write_headers(qlog, fp)

                for s3_path in r['result_location']:
                    # In Python 3,
                    # If the delim is None, fp should be in binary mode because
                    # boto expects it to be.
                    # If the delim is not None, then both text and binary modes
                    # work.

                    _download_to_local(boto_conn, s3_path, fp, num_result_dir, delim=delim)
            else:
                fp.write(",".join(r['result_location']))


class HiveCommand(Command):
    usage = ("hivecmd <submit|run> [options]")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-q", "--query", dest="query", help="query string")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where hive query to run is stored. Can be S3 URI or local file path")

    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--sample_size", dest="sample_size",
                         help="size of sample in bytes on which to run query")

    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this query")

    optparser.add_option("--pool", dest="pool",
                         help="Specify the Fairscheduler pool name for the command to use")

    optparser.add_option("--hive-version", dest="hive_version",
                         help="Specifies the hive version to be used. eg: 0.13,1.2,etc.")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true", dest="print_logs_live",
                         default=False, help="Fetch logs and print them to stderr while command is running.")
    optparser.add_option("--retry", dest="retry", default=0, choices=[1, 2, 3], help="Number of retries for a job")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.query is None and options.script_location is None:
                raise ParseError("One of query or script location"
                                 " must be specified",
                                 cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.script_location is not None:
            if options.query is not None:
                raise ParseError(
                    "Both query and script_location cannot be specified",
                    cls.optparser.format_help())

            if not _is_cloud_url(options.script_location):

                # script location is local file

                try:
                    q = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())
                options.script_location = None
                options.query = q

        if options.macros is not None:
            options.macros = json.loads(options.macros)
        v = vars(options)
        v["command_type"] = "HiveCommand"
        return v


class SqlCommand(Command):
    usage = ("sqlcmd <submit|run> [options]")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-q", "--query", dest="query", help="query string")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where hive query to run is stored. Can be S3 URI or local file path")

    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--sample_size", dest="sample_size",
                         help="size of sample in bytes on which to run query")

    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this query")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true", dest="print_logs_live",
                         default=False, help="Fetch logs and print them to stderr while command is running.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.query is None and options.script_location is None:
                raise ParseError("One of query or script location"
                                 " must be specified",
                                 cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.script_location is not None:
            if options.query is not None:
                raise ParseError(
                    "Both query and script_location cannot be specified",
                    cls.optparser.format_help())

            if not _is_cloud_url(options.script_location):

                # script location is local file

                try:
                    q = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())
                options.script_location = None
                options.query = q

        if options.macros is not None:
            options.macros = json.loads(options.macros)
        v = vars(options)
        v["command_type"] = "SqlCommand"
        return v


class SparkCommand(Command):
    usage = ("sparkcmd <submit|run> [options]")
    allowedlanglist = ["python", "scala", "R"]

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("--program", dest="program", help=SUPPRESS_HELP)

    optparser.add_option("--cmdline", dest="cmdline", help="command line for Spark")

    optparser.add_option("--sql", dest="sql", help="sql for Spark")

    optparser.add_option("--note-id", dest="note_id", help="Id of the Notebook to run.")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where spark program to run is stored. Has to be a local file path")

    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--cluster-label", dest="label", help="the label of the cluster to run the command on")

    optparser.add_option("--language", dest="language", choices=allowedlanglist, help=SUPPRESS_HELP)

    optparser.add_option("--app-id", dest="app_id", type=int,
                         help="The Spark Job Server app id to submit this snippet to.")

    optparser.add_option("--notify", action="store_true", dest="can_notify", default=False,
                         help="sends an email on command completion")

    optparser.add_option("--name", dest="name", help="Assign a name to this query")

    optparser.add_option("--pool", dest="pool",
                         help="Specify the Fairscheduler pool name for the command to use")

    optparser.add_option("--arguments", dest="arguments", help="Spark Submit Command Line Options")

    optparser.add_option("--user_program_arguments", dest="user_program_arguments", help="Arguments for User Program")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true", dest="print_logs_live",
                         default=False, help="Fetch logs and print them to stderr while command is running.")
    optparser.add_option("--retry", dest="retry", default=0, help="Number of retries")

    @classmethod
    def validate_program(cls, options):
        bool_program = options.program is not None
        bool_other_options = options.script_location is not None or options.cmdline is not None or options.sql is not None or options.note_id is not None

        # if both are false then no option is specified ==> raise ParseError
        # if both are true then atleast two option specified ==> raise ParseError
        if bool_program == bool_other_options:
            raise ParseError(
                "Exactly One of script location or program or cmdline or sql or note_id should be specified",
                cls.optparser.format_help())
        if bool_program:
            if options.language is None:
                raise ParseError("Unspecified language for Program", cls.optparser.format_help())

    @classmethod
    def validate_cmdline(cls, options):
        bool_cmdline = options.cmdline is not None
        bool_other_options = options.script_location is not None or options.program is not None or options.sql is not None or options.note_id is not None

        # if both are false then no option is specified ==> raise ParseError
        # if both are true then atleast two option specified ==> raise ParseError
        if bool_cmdline == bool_other_options:
            raise ParseError(
                "Exactly One of script location or program or cmdline or sql or note_id should be specified",
                cls.optparser.format_help())
        if bool_cmdline:
            if options.language is not None:
                raise ParseError("Language cannot be specified with the commandline option",
                                 cls.optparser.format_help())
            if options.app_id is not None:
                raise ParseError("app_id cannot be specified with the commandline option", cls.optparser.format_help())

    @classmethod
    def validate_sql(cls, options):
        bool_sql = options.sql is not None
        bool_other_options = options.script_location is not None or options.program is not None or options.cmdline is not None or options.note_id is not None

        # if both are false then no option is specified => raise PraseError
        # if both are true then atleast two option specified => raise ParseError
        if bool_sql == bool_other_options:
            raise ParseError(
                "Exactly One of script location or program or cmdline or sql or note_id should be specified",
                cls.optparser.format_help())
        if bool_sql:
            if options.language is not None:
                raise ParseError("Language cannot be specified with the 'sql' option", cls.optparser.format_help())

    @classmethod
    def validate_script_location(cls, options):
        bool_script_location = options.script_location is not None
        bool_other_options = options.program is not None or options.cmdline is not None or options.sql is not None or options.note_id is not None

        # if both are false then no option is specified ==> raise ParseError
        # if both are true then atleast two option specified ==> raise ParseError
        if bool_script_location == bool_other_options:
            raise ParseError(
                "Exactly One of script location or program or cmdline or sql or note_id should be specified",
                cls.optparser.format_help())

        if bool_script_location:
            if options.language is not None:
                raise ParseError("Both script location and language cannot be specified together",
                                 cls.optparser.format_help())
            # for now, aws script_location is not supported and throws an error
            fileName, fileExtension = os.path.splitext(options.script_location)
            # getting the language of the program from the file extension
            if fileExtension == ".py":
                options.language = "python"
            elif fileExtension == ".scala":
                options.language = "scala"
            elif fileExtension == ".R":
                options.language = "R"
            elif fileExtension == ".sql":
                options.language = "sql"
            else:
                raise ParseError(
                    "Invalid program type %s. Please choose one from python, scala, R or sql." % str(fileExtension),
                    cls.optparser.format_help())

            if not _is_cloud_url(options.script_location):

                # script location is local file so set the program as the text from the file

                try:
                    q = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())

                options.script_location = None
                if options.language == "sql":
                    options.sql = q
                    options.language = None
                else:
                    options.program = q

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """
        try:
            (options, args) = cls.optparser.parse_args(args)
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        SparkCommand.validate_program(options)
        SparkCommand.validate_script_location(options)
        SparkCommand.validate_cmdline(options)
        SparkCommand.validate_sql(options)

        if options.macros is not None:
            options.macros = json.loads(options.macros)

        v = vars(options)
        v["command_type"] = "SparkCommand"
        return v


class PrestoCommand(Command):
    usage = ("prestocmd <submit|run> [options]")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-q", "--query", dest="query", help="query string")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where presto query to run is stored. Can be S3 URI or local file path")

    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this query")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true", dest="print_logs_live",
                         default=False, help="Fetch logs and print them to stderr while command is running.")
    optparser.add_option("--retry", dest="retry", default=0, choices=[1, 2, 3], help="Number of retries for a job")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.query is None and options.script_location is None:
                raise ParseError("One of query or script location"
                                 " must be specified",
                                 cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.script_location is not None:
            if options.query is not None:
                raise ParseError(
                    "Both query and script_location cannot be specified",
                    cls.optparser.format_help())

            if not _is_cloud_url(options.script_location):

                # script location is local file
                try:
                    q = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())
                options.script_location = None
                options.query = q

        if options.macros is not None:
            options.macros = json.loads(options.macros)
        v = vars(options)
        v["command_type"] = "PrestoCommand"
        return v


class HadoopCommand(Command):
    subcmdlist = ["jar", "s3distcp", "streaming"]
    usage = "hadoopcmd <submit|run> [options] <%s> <arg1> [arg2] ..." % "|".join(subcmdlist)

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--pool", dest="pool",
                         help="Specify the Fairscheduler pool name for the command to use")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true", dest="print_logs_live",
                         default=False, help="Fetch logs and print them to stderr while command is running.")
    optparser.add_option("--retry", dest="retry", default=0, choices=[1, 2, 3], help="Number of retries for a job")

    optparser.disable_interspersed_args()

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """
        parsed = {}

        try:
            (options, args) = cls.optparser.parse_args(args)
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        parsed['label'] = options.label
        parsed['can_notify'] = options.can_notify
        parsed['name'] = options.name
        parsed['tags'] = options.tags
        parsed["command_type"] = "HadoopCommand"
        parsed['print_logs'] = options.print_logs
        parsed['print_logs_live'] = options.print_logs_live
        parsed['pool'] = options.pool

        if len(args) < 2:
            raise ParseError("Need at least two arguments", cls.usage)

        subcmd = args.pop(0)
        if subcmd not in cls.subcmdlist:
            raise ParseError("First argument must be one of <%s>" %
                             "|".join(cls.subcmdlist))

        parsed["sub_command"] = subcmd
        parsed["sub_command_args"] = " ".join("'" + str(a) + "'" for a in args)

        return parsed


class ShellCommand(Command):
    usage = ("shellcmd <submit|run> [options] [arg1] [arg2] ...")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-s", "--script", dest="inline", help="inline script that can be executed by bash")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where bash script to run is stored. Can be S3 URI or local file path")

    optparser.add_option("-i", "--files", dest="files",
                         help="List of files [optional] Format : file1,file2 (files in s3 bucket) These files will be copied to the working directory where the command is executed")

    optparser.add_option("-a", "--archives", dest="archives",
                         help="List of archives [optional] Format : archive1,archive2 (archives in s3 bucket) These are unarchived in the working directory where the command is executed")

    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--pool", dest="pool",
                         help="Specify the Fairscheduler pool name for the command to use")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true", dest="print_logs_live",
                         default=False, help="Fetch logs and print them to stderr while command is running.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.inline is None and options.script_location is None:
                raise ParseError("One of script or it's location"
                                 " must be specified",
                                 cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.script_location is not None:
            if options.inline is not None:
                raise ParseError(
                    "Both script and script_location cannot be specified",
                    cls.optparser.format_help())

            if not _is_cloud_url(options.script_location):

                # script location is local file

                try:
                    s = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())
                options.script_location = None
                options.inline = s

            if (args is not None) and (len(args) > 0):
                if options.inline is not None:
                    raise ParseError(
                        "Extra arguments can only be "
                        "supplied with a script_location in S3 right now",
                        cls.optparser.format_help())

                setattr(options, 'parameters',
                        " ".join([pipes.quote(a) for a in args]))

        else:
            if (args is not None) and (len(args) > 0):
                raise ParseError(
                    "Extra arguments can only be supplied with a script_location",
                    cls.optparser.format_help())

        v = vars(options)
        v["command_type"] = "ShellCommand"
        return v


class PigCommand(Command):
    usage = ("pigcmd <submit|run> [options] [key1=value1] [key2=value2] ...")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-s", "--script", dest="latin_statements",
                         help="latin statements that has to be executed")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where bash script to run is stored. Can be S3 URI or local file path")

    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--pool", dest="pool",
                         help="Specify the Fairscheduler pool name for the command to use")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true", dest="print_logs_live",
                         default=False, help="Fetch logs and print them to stderr while command is running.")
    optparser.add_option("--retry", dest="retry", choices=[1, 2, 3], default=0, help="Number of retries for a job")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.latin_statements is None and options.script_location is None:
                raise ParseError("One of script or it's location"
                                 " must be specified",
                                 cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.script_location is not None:
            if options.latin_statements is not None:
                raise ParseError(
                    "Both script and script_location cannot be specified",
                    cls.optparser.format_help())

            if not _is_cloud_url(options.script_location):

                # script location is local file

                try:
                    s = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())
                options.script_location = None
                options.latin_statements = s

            if (args is not None) and (len(args) > 0):
                if options.latin_statements is not None:
                    raise ParseError(
                        "Extra arguments can only be "
                        "supplied with a script_location in S3 right now",
                        cls.optparser.format_help())

                p = {}
                for a in args:
                    kv = a.split('=')
                    if len(kv) != 2:
                        raise ParseError("Arguments to pig script must be of this format k1=v1 k2=v2 k3=v3...")
                    p[kv[0]] = kv[1]
                setattr(options, 'parameters', p)

        else:
            if (args is not None) and (len(args) > 0):
                raise ParseError(
                    "Extra arguments can only be supplied with a script_location",
                    cls.optparser.format_help())

        v = vars(options)
        v["command_type"] = "PigCommand"
        return v


class DbExportCommand(Command):
    usage = ("dbexportcmd <submit|run> [options]")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-m", "--mode", dest="mode",
                         help="Can be 1 for Hive export or 2 for HDFS/S3 export")
    optparser.add_option("--schema", help="Db schema name, assumed accordingly by database if not specified",
                         default=None, dest="schema")
    optparser.add_option("--hive_table", dest="hive_table",
                         help="Mode 1: Name of the Hive Table from which data will be exported")
    optparser.add_option("--partition_spec", dest="partition_spec",
                         help="Mode 1: (optional) Partition specification for Hive table")
    optparser.add_option("--dbtap_id", dest="dbtap_id",
                         help="Modes 1 and 2: DbTap Id of the target database in Qubole")
    optparser.add_option("--db_table", dest="db_table",
                         help="Modes 1 and 2: Table to export to in the target database")
    optparser.add_option("--use_customer_cluster", dest="use_customer_cluster", default=False,
                         help="Modes 1 and 2: To use cluster to run command ")
    optparser.add_option("--customer_cluster_label", dest="customer_cluster_label",
                         help="Modes 1 and 2: the label of the cluster to run the command on")
    optparser.add_option("--db_update_mode", dest="db_update_mode",
                         help="Modes 1 and 2: (optional) can be 'allowinsert' or "
                              "'updateonly'. If updateonly is "
                              "specified - only existing rows are updated. If allowinsert "
                              "is specified - then existing rows are updated and non existing "
                              "rows are inserted. If this option is not specified - then the "
                              "given the data will be appended to the table")
    optparser.add_option("--db_update_keys", dest="db_update_keys",
                         help="Modes 1 and 2: Columns used to determine the uniqueness of rows for "
                              "'updateonly' mode")
    optparser.add_option("--export_dir", dest="export_dir",
                         help="Mode 2: HDFS/S3 location from which data will be exported")
    optparser.add_option("--fields_terminated_by", dest="fields_terminated_by",
                         help="Mode 2: Hex of the char used as column separator "
                              "in the dataset, for eg. \0x20 for space")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--additional_options",
                         help="Additional Sqoop options which are needed enclose options in double or single quots e.g. '--map-column-hive id=int,data=string'")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true", dest="print_logs_live",
                         default=False, help="Fetch logs and print them to stderr while command is running.")
    optparser.add_option("--retry", dest="retry", default=0, choices=[1, 2, 3], help="Number of retries for a job")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.mode not in ["1", "2"]:
                raise ParseError("mode must be either '1' or '2'",
                                 cls.optparser.format_help())

            if (options.dbtap_id is None) or (options.db_table is None):
                raise ParseError("dbtap_id and db_table are required",
                                 cls.optparser.format_help())

            if options.mode is "1":
                if options.hive_table is None:
                    raise ParseError("hive_table is required for mode 1",
                                     cls.optparser.format_help())
            elif options.export_dir is None:  # mode 2
                raise ParseError("export_dir is required for mode 2",
                                 cls.optparser.format_help())

            if options.db_update_mode is not None:
                if options.db_update_mode not in ["allowinsert", "updateonly"]:
                    raise ParseError("db_update_mode should either be left blank for append "
                                     "mode or be 'updateonly' or 'allowinsert'",
                                     cls.optparser.format_help())
                if options.db_update_mode is "updateonly":
                    if options.db_update_keys is None:
                        raise ParseError("db_update_keys is required when db_update_mode "
                                         "is 'updateonly'",
                                         cls.optparser.format_help())
                elif options.db_update_keys is not None:
                    raise ParseError("db_update_keys is used only when db_update_mode "
                                     "is 'updateonly'",
                                     cls.optparser.format_help())

        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        v = vars(options)
        v["command_type"] = "DbExportCommand"
        return v


class DbImportCommand(Command):
    usage = "dbimportcmd <submit|run> [options]"

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-m", "--mode", dest="mode",
                         help="Can be 1 for Hive export or 2 for HDFS/S3 export")
    optparser.add_option("--schema", help="Db schema name, assumed accordingly by database if not specified",
                         default=None, dest="schema")
    optparser.add_option("--hive_table", dest="hive_table",
                         help="Mode 1: Name of the Hive Table from which data will be exported")
    optparser.add_option("--hive_serde", dest="hive_serde",
                         help="Output format of the Hive Table")
    optparser.add_option("--dbtap_id", dest="dbtap_id",
                         help="Modes 1 and 2: DbTap Id of the target database in Qubole")
    optparser.add_option("--db_table", dest="db_table",
                         help="Modes 1 and 2: Table to export to in the target database")
    optparser.add_option("--use_customer_cluster", dest="use_customer_cluster", default=False,
                         help="Modes 1 and 2: To use cluster to run command ")
    optparser.add_option("--customer_cluster_label", dest="customer_cluster_label",
                         help="Modes 1 and 2: the label of the cluster to run the command on")
    optparser.add_option("--where_clause", dest="db_where",
                         help="Mode 1: where clause to be applied to the table before extracting rows to be imported")
    optparser.add_option("--parallelism", dest="db_parallelism",
                         help="Mode 1 and 2: Number of parallel threads to use for extracting data")
    optparser.add_option("--extract_query", dest="db_extract_query",
                         help="Modes 2: SQL query to be applied at the source database for extracting data. "
                              "$CONDITIONS must be part of the where clause")
    optparser.add_option("--boundary_query", dest="db_boundary_query",
                         help="Mode 2: query to be used get range of rowids to be extracted")
    optparser.add_option("--split_column", dest="db_split_column",
                         help="column used as rowid to split data into range")
    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")
    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")
    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")
    optparser.add_option("--additional_options",
                         help="Additional Sqoop options which are needed enclose options in double or single quotes")
    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true", dest="print_logs_live",
                         default=False, help="Fetch logs and print them to stderr while command is running.")
    optparser.add_option("--retry", dest="retry", default=0, choices=[1, 2, 3], help="Number of retries for a job")
    optparser.add_option("--partition_spec", dest="part_spec", default=None,
                         help="Mode 1: (optional) Partition specification for Hive table")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.mode not in ["1", "2"]:
                raise ParseError("mode must be either '1' or '2'",
                                 cls.optparser.format_help())

            if (options.dbtap_id is None) or (options.db_table is None):
                raise ParseError("dbtap_id and db_table are required",
                                 cls.optparser.format_help())

            # TODO: Semantic checks for parameters in mode 1 and 2

        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        v = vars(options)
        v["command_type"] = "DbImportCommand"
        return v


class CompositeCommand(Command):
    @classmethod
    def compose(cls, sub_commands, macros=None, cluster_label=None, notify=False, name=None, tags=None):
        """
        Args:
            `sub_commands`: list of sub-command dicts

        Returns:
            Dictionary that can be used in create method

        Example Usage:
            cmd1 = HiveCommand.parse(['--query', "show tables"])
            cmd2 = PigCommand.parse(['--script_location', "s3://paid-qubole/PigAPIDemo/scripts/script1-hadoop-s3-small.pig"])
            composite = CompositeCommand.compose([cmd1, cmd2])
            cmd = CompositeCommand.run(**composite)
        """
        if macros is not None:
            macros = json.loads(macros)
        return {
            "sub_commands": sub_commands,
            "command_type": "CompositeCommand",
            "macros": macros,
            "label": cluster_label,
            "tags": tags,
            "can_notify": notify,
            "name": name
        }


class DbTapQueryCommand(Command):
    usage = "dbtapquerycmd <submit|run> [options]"

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("--db_tap_id", dest="db_tap_id",
                         help="dbTap Id of the target database in Qubole")
    optparser.add_option("-q", "--query", dest="query", help="query string")
    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")
    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where query to run is stored. Can be S3 URI or local file path")
    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")
    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true", dest="print_logs_live",
                         default=False, help="Fetch logs and print them to stderr while command is running.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.db_tap_id is None:
                raise ParseError("db_tap_id is required",
                                 cls.optparser.format_help())
            if options.query is None and options.script_location is None:
                raise ParseError("query or script location is required",
                                 cls.optparser.format_help())

            if options.script_location is not None:
                if options.query is not None:
                    raise ParseError(
                        "Both query and script_location cannot be specified",
                        cls.optparser.format_help())

                if not _is_cloud_url(options.script_location):

                    # script location is local file

                    try:
                        q = open(options.script_location).read()
                    except IOError as e:
                        raise ParseError("Unable to open script location: %s" %
                                         str(e),
                                         cls.optparser.format_help())
                    options.script_location = None
                    options.query = q

        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.macros is not None:
            options.macros = json.loads(options.macros)
        v = vars(options)
        v["command_type"] = "DbTapQueryCommand"
        return v


class JupyterNotebookCommand(Command):
    usage = "jupyternotebookcmd <submit|run> [options]"

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("--path", dest="path",
                         help="Path including name of the Jupyter notebook to \
                         be run with extension.")
    optparser.add_option("--cluster-label", dest="label",
                         help="Label of the cluster on which the this command \
                         should be run. If this parameter is not specified \
                         then label = 'default' is used.")
    optparser.add_option("--arguments", dest="arguments",
                         help="Valid JSON to be sent to the notebook. Specify \
                         the parameters in notebooks and pass the parameter value \
                         using the JSON format. key is the parameter's name and \
                         value is the parameter's value. Supported types in \
                         parameters are string, integer, float and boolean.")
    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")
    optparser.add_option("--name", dest="name", help="Assign a name to this query")
    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with \
                         the query ( e.g. tag1 tag1,tag2 )")
    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")
    optparser.add_option("--timeout", dest="timeout", type="int",
                         help="Timeout for command execution in seconds")
    optparser.add_option("--retry", dest="retry", choices=['1', '2', '3'],
                         help="Number of retries for a job")
    optparser.add_option("--retry-delay", dest="retry_delay", type="int",
                         help="Time interval between the retries when a job fails.")
    optparser.add_option("--pool", dest="pool",
                         help="Specify the Fairscheduler pool name for the \
                         command to use")
    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    optparser.add_option("--print-logs-live", action="store_true",
                         dest="print_logs_live", default=False, help="Fetch logs \
                         and print them to stderr while command is running.")
    optparser.add_option("--upload-to-source", dest="upload_to_source", default='true',
                         help="Upload notebook to source after completion of \
                         execution. Specify the value as either 'true' or 'false'.\
                         Default value is 'true'.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """
        try:
            options, args = cls.optparser.parse_args(args)
            if options.path is None:
                raise ParseError("Notebook Path must be specified",
                                 cls.optparser.format_help())
            if options.arguments is not None:
                validate_json_input(options.arguments, 'Arguments', cls)
            if options.macros is not None:
                options.macros = validate_json_input(options.macros, 'Macros', cls)
            if options.retry is not None:
                options.retry = int(options.retry)
            if options.upload_to_source is not None:
                options.upload_to_source = options.upload_to_source.lower()
                if options.upload_to_source == 'true':
                    options.upload_to_source = True
                elif options.upload_to_source == 'false':
                    options.upload_to_source = False
                else:
                    msg = "Upload to Source parameter takes a value of either 'true' \
                    or 'false' only."
                    raise ParseError(msg, cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        params = vars(options)
        params["command_type"] = "JupyterNotebookCommand"
        return params


def validate_json_input(string, option_type, cls):
    """Converts String to JSON and throws ParseError if string is not valid JSON"""

    try:
        return json.loads(string)
    except ValueError as e:
        raise ParseError("Given %s is not valid JSON: %s" % (option_type, str(e)),
                         cls.optparser.format_help())


def _read_iteratively(key_instance, fp, delim):
    key_instance.open_read()
    while True:
        try:
            # Default buffer size is 8192 bytes
            data = next(key_instance)
            if sys.version_info < (3, 0, 0):
                fp.write(str(data).replace(chr(1), delim))
            else:
                import io
                if isinstance(fp, io.TextIOBase):
                    fp.buffer.write(data.replace(bytes([1]), delim.encode('utf8')))
                elif isinstance(fp, io.BufferedIOBase) or isinstance(fp, io.RawIOBase):
                    fp.write(data.replace(bytes([1]), delim.encode('utf8')))
                else:
                    raise ValueError('Only subclasses of io.TextIOBase or io.BufferedIOBase supported')
        except StopIteration:
            # Stream closes itself when the exception is raised
            return


def write_headers(qlog, fp):
    col_names = []
    qlog = json.loads(qlog)
    if qlog["QBOL-QUERY-SCHEMA"] is not None:
        qlog_hash = qlog["QBOL-QUERY-SCHEMA"].get("-1") or qlog["QBOL-QUERY-SCHEMA"][
            list(qlog["QBOL-QUERY-SCHEMA"].keys())[0]]

        for qlog_item in qlog_hash:
            col_names.append(qlog_item["ColumnName"])

        col_names = "\t".join(col_names)
        col_names += "\n"
    fp.write(col_names.encode())


def _download_to_local(boto_conn, s3_path, fp, num_result_dir, delim=None):
    '''
    Downloads the contents of all objects in s3_path into fp

    Args:
        `boto_conn`: S3 connection object

        `s3_path`: S3 path to be downloaded

        `fp`: The file object where data is to be downloaded
    '''

    # Progress bar to display download progress
    def _callback(downloaded, total):
        '''
        Call function for upload.

        `downloaded`: File size already downloaded (int)

        `total`: Total file size to be downloaded (int)
        '''
        if (total is 0) or (downloaded == total):
            return
        progress = downloaded * 100 / total
        sys.stderr.write('\r[{0}] {1}%'.format('#' * progress, progress))
        sys.stderr.flush()

    m = _URI_RE.match(s3_path)
    bucket_name = m.group(1)
    bucket = boto_conn.get_bucket(bucket_name)
    retries = 6
    if s3_path.endswith('/') is False:
        # It is a file
        key_name = m.group(2)
        key_instance = bucket.get_key(key_name)
        while key_instance is None and retries > 0:
            retries = retries - 1
            log.info("Results file is not available on s3. Retry: " + str(6 - retries))
            time.sleep(10)
            key_instance = bucket.get_key(key_name)
        if key_instance is None:
            raise Exception(
                "Results file not available on s3 yet. This can be because of s3 eventual consistency issues.")
        log.info("Downloading file from %s" % s3_path)
        if delim is None:
            try:
                key_instance.get_contents_to_file(fp)  # cb=_callback
            except boto.exception.S3ResponseError as e:
                if (e.status == 403):
                    # SDK-191, boto gives an error while fetching the objects using versions which happens by default
                    # in the get_contents_to_file() api. So attempt one without specifying version.
                    log.warn("Access denied while fetching the s3 object. Retrying without specifying the version....")
                    key_instance.open()
                    fp.write(key_instance.read())
                    key_instance.close()
                else:
                    raise
        else:
            # Get contents as string. Replace parameters and write to file.
            _read_iteratively(key_instance, fp, delim=delim)

    else:
        # It is a folder
        key_prefix = m.group(2)
        bucket_paths = bucket.list(key_prefix)
        for one_path in bucket_paths:
            name = one_path.name

            # Eliminate _tmp_ files which ends with $folder$
            if name.endswith('$folder$'):
                continue

            log.info("Downloading file from %s" % name)
            if delim is None:
                one_path.get_contents_to_file(fp)  # cb=_callback
            else:
                _read_iteratively(one_path, fp, delim=delim)
