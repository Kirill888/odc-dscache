import rasterio
from dea.aws import get_boto3_session
from dea.aws.rioenv import setup_local_env, local_env, has_local_env

from .parallel import ParallelStreamProc


__all__ = ["ParallelReader"]


class ParallelReader(object):
    """This class will process a bunch of files in parallel. You provide a
    generator of (userdata, url) tuples and a callback that takes opened
    rasterio file handle and userdata. This class deals with launching threads
    and re-using them between calls and with setting up `rasterio.Env`
    appropriate for S3 access. Callback will be called concurrently from many
    threads, it should do it's own synchronization.

    This class is roughly equivalent to this serial code:

    ```
    with rasterio.Env(**cfg):
      for userdata, url in srcs:
         with rasterio.open(url,'r') as f:
            cbk(f, userdata)
    ```

    You should create one instance of this class per app and re-use it as much
    as practical. There is a significant setup cost, and it increases almost
    linearly with more worker threads. There is large latency for processing
    first file in the worker thread, some gdal per-thread setup, so it's
    important to re-use an instance of this class rather than creating a new
    one for each request.
    """
    @staticmethod
    def _process_file_stream(src_stream,
                             on_file_cbk,
                             credentials,
                             gdal_opts=None,
                             region_name=None,
                             timer=None):

        if not has_local_env():
            setup_local_env(credentials, region_name=region_name, **gdal_opts)

        env = local_env()
        open_args = dict(sharing=False)

        if timer is not None:
            def proc(url, userdata):
                t0 = timer()
                with rasterio.open(url, 'r', **open_args) as f:
                    on_file_cbk(f, userdata, t0=t0)
        else:
            def proc(url, userdata):
                with rasterio.open(url, 'r', **open_args) as f:
                    on_file_cbk(f, userdata)

        for userdata, url in src_stream:
            with env:
                proc(url, userdata)

    def __init__(self, nthreads,
                 region_name=None,
                 **gdal_extra_opts):
        session = get_boto3_session(region_name=region_name)

        self._nthreads = nthreads
        self._pstream = ParallelStreamProc(nthreads)
        self._process_files = self._pstream.bind(ParallelReader._process_file_stream)
        self._region_name = session.region_name
        self._creds = session.get_credentials()

        self._gdal_opts = dict(VSI_CACHE=True,
                               GDAL_INGESTED_BYTES_AT_OPEN=64*1024,
                               CPL_VSIL_CURL_ALLOWED_EXTENSIONS='tif',
                               GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR')
        self._gdal_opts.update(**gdal_extra_opts)

    def warmup(self, action=None):
        """Mostly needed for benchmarking needs. Ensures that worker threads are
        started and have S3 credentials pre-loaded.

        If you need to setup some thread-local state you can supply action
        callback that will be called once in every worker thread.
        """
        def _warmup():
            if not has_local_env():
                setup_local_env(self._creds, region_name=self._region_name, **self._gdal_opts)

            with local_env() as env:
                if action:
                    action()
                return env

        return self._pstream.broadcast(_warmup)

    def process(self, stream, cbk, timer=None):
        """
        stream: (userdata, url)...
        cbk:
           file_handle, userdata -> None (ignored)|
           file_handle, userdata, t0 -> None (ignored) -- when timer is supplied

        timer: None| ()-> TimeValue

        Equivalent to this serial code, but with many concurrent threads and
        with appropriate `rasterio.Env` wrapper for S3 access

        ```
        for userdata, url in stream:
            with rasterio.open(url,'r') as f:
               cbk(f, userdata)
        ```

        if timer is set:
        ```
        for userdata, url in stream:
            t0 = timer()
            with rasterio.open(url, 'r') as f:
               cbk(f, userdata, t0=t0)
        ```
        """
        self._process_files(stream, cbk,
                            self._creds,
                            self._gdal_opts,
                            region_name=self._region_name,
                            timer=timer)
