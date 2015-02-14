from flask_restplus import fields


class ISO8601DateField(fields.DateTime):
    def __init__(self):
        super(ISO8601DateField, self).__init__('iso8601')

    def format(self, value):
        rep = super(ISO8601DateField, self).format(value)
        return rep[:10]