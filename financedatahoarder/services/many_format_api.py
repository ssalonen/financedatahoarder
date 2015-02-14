from flask import request
from werkzeug.exceptions import NotAcceptable
from flask.ext.restplus import Api


class ManyFormatApi(Api):
    FORMAT_MIMETYPE_MAP = {
        "csv": "text/csv",
        "json": "application/json"
        # Add other mimetypes as desired here
    }

    def mediatypes(self):
        """Allow all resources to have their representation
        overriden by the `format` URL argument"""

        preferred_response_type = []
        format = request.args.get("format")
        if format:
            mimetype = self.FORMAT_MIMETYPE_MAP.get(format)
            preferred_response_type.append(mimetype)
            if not mimetype:
                raise NotAcceptable()
        return preferred_response_type + super(ManyFormatApi, self).mediatypes()