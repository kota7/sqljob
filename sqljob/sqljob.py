# -*- coding: utf-8 -*-

from logging import getLogger
from datetime import datetime
from contextlib import contextmanager
import csv
import os
import io
import threading
import multiprocessing
import warnings

import pandas as pd
import sqlalchemy

logger = getLogger(__name__)



class Connector:
    """
    Init Args:
      connector: an object with `connect` method, typically a module for a specific SQL dialect
      query: str, SQL statument
      args: tuple, unnamed paramters to pass to the connect method
      kwargs: dict, named paramters to pass to the connect method
    """
    def __init__(self, connect_module, *args, **kwargs):
        self.connect_module = connect_module
        self.args = args
        self.kwargs = kwargs

    @contextmanager
    def connect(self):
        conn = self.connect_module.connect(*self.args, **self.kwargs)
        try:
            yield conn
        finally:
            conn.close()


class SqlJob:
    """
    Create SQL Job object

    Init Args:
      query: str, SQL statument
      connector: an object with `connect` method that can be run without arguments,
                 typically a `Connector` object or sqlalchemy engine
      params: tuple, parameters passed along the query
      manyparams: bool, indicates that the query should be executed with `executemany`
      postcommit: bool, if set, run commit method (if available) after the query execution
      backend: "threading" or "multiprocessing", indicates the backend module for running a child task
      csvfile: str or None, if given, query result is written to this file
      max_df_rows: int, maximum number of rows to keep in the result data frame
      logquery: bool, if set, query statement is added to the log message
      logquery_params: bool, if set, query parameters are added to the log message
    """
    current_jobid = 0
    result_dir = os.path.abspath("./sqljob-results")

    def __init__(self, query, connector, params=(), manyparams=False, postcommit=True,
                 backend="threading", csvfile=None, picklefile=None, max_df_rows=10000,
                 logquery=True, logquery_params=False):

        self.connector = connector
        self.query = query
        self.params = params
        self.manyparams = manyparams
        self.postcommit = postcommit
        self.backend = backend
        assert backend in ("threading", "multiprocessing"), "Invalid backend: '{}'".format(backend)
        self.max_df_rows = max_df_rows
        self.logquery = logquery
        self.logquery_params = logquery_params

        self.jobid = SqlJob.current_jobid + 1
        SqlJob.current_jobid += 1
        
        thistime = datetime.now().strftime("%y%m%d_%H%M%S")
        if csvfile is None:
            csvfile = "job_{}_{}.csv".format(self.jobid, thistime)
        if picklefile is None:
            picklefile = "job_{}_{}.pkl".format(self.jobid, thistime)
        self.csvfile = os.path.join(SqlJob.result_dir, csvfile)
        self.picklefile = os.path.join(SqlJob.result_dir, picklefile)

        self.worker = None  # placeholder to store the worker object (Thread or Process)
        self._result_df = None  # placeholder to keep the query outcome
    

    def _make_worker(self):
        args = (self.connector, self.query)
        kwargs = {"params": self.params, "manyparams": self.manyparams, "postcommit": self.postcommit,
                  "csvfile": self.csvfile, "picklefile": self.picklefile, "max_df_rows": self.max_df_rows,
                  "logquery": self.logquery, "logquery_params": self.logquery_params}
        #logger.debug("%s", args)
        #logger.debug("%s", kwargs)
        if self.backend=="threading":
            self.worker = threading.Thread(target=_sql_task, args=args, kwargs=kwargs, name="sqljob_{}".format(self.jobid))
        elif self.backend=="multiprocessing":
            warnings.warn("multiprocessing backend is experimental")
            self.worker = multiprocessing.Process(target=_sql_task, args=args, kwargs=kwargs, name="sqljob_{}".format(self.jobid))
            
    def start(self):
        self._make_worker()
        
        logger.info("Start SQL worker (id=%s)", self.jobid)
        self.worker.start()
        return self
    
    def wait(self, timeout=None):
        self.worker.join(timeout)
        return self
    
    def running(self):
        return self.worker.is_alive()

    @property
    def result_df(self):
        if self.running():
            print("Job is still running")
            return None
        elif self._result_df is not None:
            return self._result_df.copy()
        else:
            if os.path.isfile(self.picklefile):
                df = pd.read_pickle(self.picklefile)
                self._result_df = df
                return df.copy()
            else:
                print("No data frame result for this job")
                return None

def sqljob(query, connector, params=(), manyparams=False, postcommit=False, backend="threading", 
           csvfile=None, picklefile=None, max_df_rows=10000, logquery=True, logquery_params=False):
    job = SqlJob(query, connector, params=params, manyparams=manyparams, postcommit=postcommit,
                 backend=backend, csvfile=csvfile, picklefile=picklefile, max_df_rows=max_df_rows,
                 logquery=logquery, logquery_params=logquery_params)
    job.start()
    return job


def _sql_task(connector, query, params=(), manyparams=False, postcommit=True,
              csvfile=None, picklefile=None, max_df_rows=10000, logquery=True, logquery_params=False):
    # defines a generic sql task
    logger.info("Start SQL job")
    logger.debug("Result files are: csv '%s' and pickle '%s'", csvfile, picklefile)
    os.makedirs(os.path.abspath(os.path.dirname(csvfile)), exist_ok=True)
    os.makedirs(os.path.abspath(os.path.dirname(picklefile)), exist_ok=True)

    logger.debug("Establishing connection to the database")
    if isinstance(connector, str):
        engine = sqlalchemy.create_engine(connector)
    else:
        engine = connector
    assert hasattr(engine, "connect") and callable(engine.connect), "Connector ({}) does not have connect method".format(type(engine))
    with engine.connect() as conn:
        try:
            c = conn.cursor()
        except Exception as e:
            logger.debug("`cursor` method is not available, will execute query on the connection directly")
            c = conn
        logger.info("Start running query%s%s",
                    "\n" + query if logquery else "",
                    "\n with" + str(params) if logquery_params else "")

        t1 = datetime.now()
        if manyparams:
            result = c.executemany(query, params)
        else:
            result = c.execute(query, params)
        if postcommit:
            logger.debug("Postcommit mode on")
            if hasattr(conn, "commit"):
                conn.commit()
                logger.debug("Changes commited")
            else:
                logger.debug("The connection (%s) has no commit method", type(conn))
        t2 = datetime.now()
        logger.info("Finish running query (Elapsed: %s)", t2-t1)
        logger.info("%s rows has been affected", getattr(result, "rowcount", "???"))
        
        logger.info("Fetch and write result table if any")
        _fetch_and_write(result, csvfile, picklefile, max_df_rows)
    logger.info("End SQL job")


def _get_header(cursor):
    """
    Returns column names from the query result object (cursor, result proxy etc.)
    """
    if hasattr(cursor, "description"):
        try:
            header = [d[0] for d in cursor.description]
            logger.debug("Header is retrieved from the first elements of description field")
            return header
        except Exception as e:
            logger.debug("Failed to fetch the header from description field: '%s'", e)
    if hasattr(cursor, "keys"):
        try:
            header = list(cursor.keys())
            logger.debug("Header is retrieved by the `keys` method")
            return header
        except Exception as e:
            logger.debug("Failed to fetch the header by keys method: '%s'", e)
    logger.debug("No header is found")
    return None


def _fetch_and_write(cursor, csvfile=None, picklefile=None, max_df_rows=10000):
    """
    Write rows to csvfile and pickefile at the same time
    """
    logger.debug("Start fetching the result set")
    # delete existing csv file if any
    if csvfile is not None and os.path.isfile(csvfile):
        os.unlink(csvfile)
    if picklefile is not None and os.path.isfile(picklefile):
        os.unlink(picklefile)

    # find column names if any
    header = _get_header(cursor)
    if header is None:
        logger.info("No column names found")
    else:
        logger.debug("Column names: %s", header)

    # open csv file
    if csvfile is not None and header is not None:
        logger.debug("Writing header to CSV file '%s'", csvfile)
        with open(csvfile, "wt", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
    
    # first, we will check if this result has any data in it
    try:
        row = cursor.fetchone()
        if row is not None:
            do_iterate = True
            rows = [row]
            if csvfile is not None:
                logger.debug("Writing first row to CSV file '%s'", csvfile)
                with open(csvfile, "at", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(row)
        else:
            logger.debug("fetchone obtained None, no result set in the outcome")
            do_iterate = False
            rows = []
    except Exception as e:
        if type(e) == StopIteration:
            logger.debug("fetchone method obtained stop iteration error")
        else:
            logger.debug("fetchone method obtained error: '%s'", e)
        logger.info("No result set in the query outcome")
        rows = []
        do_iterate = False

    # get remaining rows
    if do_iterate:
        if csvfile is not None:
            logger.debug("Start writing remaining rows to CSV file '%s'", csvfile)
            f = open(csvfile, "at", newline="")
            writer = csv.writer(f)
        else:
            f = None
            writer = None

        for i, row in enumerate(cursor):
            # note: we already have one record
            if i+1 < max_df_rows:
                rows.append(row)
            elif i+1 == max_df_rows:
                logger.info("Data frame is truncated to %d rows", max_df_rows)
            if writer is not None:
                writer.writerow(row)
        if f is not None:
            logger.info("Finish writing to CSV file '%s'", csvfile)
            f.close()

    # write pickle only if there are some rows or header
    if (picklefile is not None) and (len(rows) > 0 or header is not None):
        df = pd.DataFrame(rows, columns=header)
        df.to_pickle(picklefile)
        logger.info("Data frame is written to '%s'", picklefile)
    else:
        logger.debug("Nothing to write to pickle because there is no row or header")

    logger.debug("Finish fetching the result set")
    