import re
from datetime import datetime, timedelta


def ds_format_v2(ds, input_format, output_format, days=None):
    if days is None:
        return datetime.strptime(str(ds), input_format).strftime(output_format)
    else:
        dt = datetime.strptime(str(ds), input_format) + timedelta(days=days)
        return dt.strftime(output_format)
