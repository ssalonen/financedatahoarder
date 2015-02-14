from textwrap import dedent
import os
import logging
import logging.config

# Configure logging with uwsgi
logging_cfg_path = os.environ.get('LOGGING_INI', None)
if logging_cfg_path:
    logging.config.fileConfig(logging_cfg_path)
    print 'Using logging configuration: {}'.format(logging_cfg_path)
else:
    print 'No LOGGING_INI set. Using default logging configuration'


from cStringIO import StringIO
import json
import sys

from financedatahoarder.services.fields import ISO8601DateField
from financedatahoarder.services.data_access_api import NonCachingAsyncRequestsClient
from financedatahoarder.services.many_format_api import ManyFormatApi
import numpy as np
import pandas as pd
from flask import Flask, make_response
from flask.ext.restplus import Api, Resource, fields, marshal_with
from flask_restful import inputs







# TODO: query HTTP 200 responses using the pywb api, at least there is this (html format)
# curl -H "Accept: application/json" "localhost:8080/replay/pywb-cdx/*/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL"
# pywb code seems to suggest that there is fancier api directly querying HTTP 200 response...but is it exposed? See https://github.com/ikreymer/pywb/blob/master/pywb/webapp/query_handler.py

# Parse example from the html page!
# date_json = x.xpath('//tr[td[2]/text() = "200"]//script')
# link = x.xpath('//tr[td[2]/text() = "200"]//a/@href')

# TODO: support predefined, configurable set of URLs

# curl -H "Accept: application/json" "http://127.0.0.1:5000/instruments/?date_interval=2015-03-01%2F2015-03-15&url=http%3A%2F%2Fwww.morningstar.fi%2Ffi%2Ffunds%2Fsnapshot%2Fsnapshot.aspx%3Fid%3DF00000UF2B&url=http%3A%2F%2Fwww.morningstar.fi%2Ffi%2Ffunds%2Fsnapshot%2Fsnapshot.aspx%3Fid%3D0P0000GGNP"
# curl -H "Accept: text/csv" "http://127.0.0.1:5000/instruments/?date_interval=2015-03-01%2F2015-03-15&url=http%3A%2F%2Fwww.morningstar.fi%2Ffi%2Ffunds%2Fsnapshot%2Fsnapshot.aspx%3Fid%3DF00000UF2B&url=http%3A%2F%2Fwww.morningstar.fi%2Ffi%2Ffunds%2Fsnapshot%2Fsnapshot.aspx%3Fid%3D0P0000GGNP"
# curl "http://127.0.0.1:5000/instruments/?date_interval=2015-03-01%2F2015-03-22&url=http%3A%2F%2Fwww.morningstar.fi%2Ffi%2Ffunds%2Fsnapshot%2Fsnapshot.aspx%3Fid%3DF00000UF2B&url=http%3A%2F%2Fwww.morningstar.fi%2Ffi%2Ffunds%2Fsnapshot%2Fsnapshot.aspx%3Fid%3D0P0000GGNP&url=http%3a%2f%2ftools.morningstar.fi%2ffi%2fstockreport%2fdefault.aspx%3fSecurityToken%3d0P0000A5Z8%255D3%255D0%255DE0WWE%24%24ALL&url=http%3a%2f%2ftools.morningstar.fi%2ffi%2fstockreport%2fdefault.aspx%3fSecurityToken%3d0P0000A5Z8%255D3%255D0%255DE0WWE%24%24ALL"
from financedatahoarder.services.utils import dataframe_from_list_of_dicts




app = Flask(__name__)
# Load the configuration from the instance folder
app.config.from_pyfile('config.py')
# app.config.from_envvar('APP_CONFIG_FILE)


class ErrorHandlingApi(Api):
    def handle_error(self, e):
        logging.getLogger(__name__).error('Error occurred in the service: {}'.format(e), exc_info=True)
        return super(ErrorHandlingApi, self).handle_error(e)


#api = ManyFormatApi(app)
api = ErrorHandlingApi(app)

client = NonCachingAsyncRequestsClient(app.config['BASE_REPLAY_URL'], app.config['GREQUESTS_POOL_SIZE'],
                                       expire_after=app.config['CACHE_EXPIRE_AFTER'])


@api.representation('application/json')
def output_json(data, code, headers=None):
    """Makes a Flask response with a JSON encoded body"""
    resp = make_response(json.dumps(data), code)
    resp.headers.extend(headers or {})
    return resp


@api.representation('text/csv')
def output_csv(data, code, headers=None):
    # Handle swagger metadata
    if 'message' in data:
        # E.g. Bad requests
        resp = make_response(
            'Exception ({}): '.format(code) + ', '.join(['{}: {}'.format(k, v) for k, v in data.iteritems()]), code)
        resp.headers.extend(headers or {})
        return resp
    if 'swagger' in data:
        return output_json(data, code, headers)
    elif isinstance(data, dict) and set(data.keys()) == {'status', 'message'}:
        # Unhandled error!
        df = pd.DataFrame(data, index=[0])
    else:
        df = dataframe_from_list_of_dicts(data)
    buf = StringIO()
    df.to_csv(buf, index=False)
    resp = make_response(buf.getvalue(), code)
    resp.headers.extend(headers or {})
    return resp


parser = api.parser()
parser.add_argument('format', type=str, default='csv')
parser.add_argument('url', type=inputs.url, action='append', default=[], help='Url of instruments', dest='urls')
parser.add_argument('date_interval', type=inputs.iso8601interval, help='Date or date interval to query for',
                    required=True)

InstrumentValueEntry = api.model('InstrumentValueEntry', {
    'instrument_url': fields.String,
    'value': fields.Float(np.nan),
    'value_date': ISO8601DateField  #fields.DateTime('iso8601')
})


@api.route('/instruments/')
class SingleInstrumentResource(Resource):

    @api.doc(parser=parser)
    @marshal_with(InstrumentValueEntry)
    def get(self):
        """Get instruments, as specified by the urls, for a given date interval"""
        args = parser.parse_args()
        val = client.query_key_stats(args.date_interval, args.urls)
        if not val:
            if len(args.urls):
                api.abort(404, message='Could not find instrument(s)')
            else:
                api.abort(404, message='No instruments given as input')
        else:
            return val


def main_test():
    import tempfile
    PROFILE=True

    f = tempfile.NamedTemporaryFile(prefix='rest_server_logging_', delete=False)
    f.write(dedent("""
    [loggers]
    keys = root

    [logger_root]
    handlers = stdout
    level = DEBUG

    [handlers]
    keys = stdout

    [formatters]
    keys=simpleFormatter

    [handler_stdout]
    class = StreamHandler
    args = (sys.stdout,)
    formatter=simpleFormatter

    [formatter_simpleFormatter]
    format=%(asctime)s - %(name)s - %(levelname)s - %(message)s
    datefmt=%Y-%m-%d %H:%M:%S
    """))
    logging_ini = f.name
    f.close()
    try:
        logging.config.fileConfig(logging_ini)
        if PROFILE:
            from werkzeug.contrib.profiler import ProfilerMiddleware
            app.config['PROFILE'] = True
            app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[30],
                                              profile_dir='/tmp/')

        app.run(debug=True)
    finally:
        try:
            os.remove(logging_ini)
        except WindowsError:
            pass


if __name__ == '__main__':
    main_test()