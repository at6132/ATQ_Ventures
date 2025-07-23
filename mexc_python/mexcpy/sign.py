import json
import hashlib
from time import time
import os

def get_data(info, auth):
    ts = str(int(time() * 1000))
    chash = os.urandom(16).hex()
    data_info = {} if info is None or isinstance(info, list) else info
    data = {
        **data_info,
        "chash": chash,
        "ts": ts,
    }

    form_data = info if isinstance(info, list) else data

    hash = get_sign(auth, json.dumps(form_data), ts)
    return data, hash, ts

def get_md5(string):
    return hashlib.md5(string.encode("utf-8")).hexdigest()

def get_g(auth, ts):
    md5_hash = get_md5(auth + ts)
    return md5_hash[7:], ts

def get_sign(auth, formdata, ts):
    g, current_ts = get_g(auth, ts)
    return get_md5(current_ts + formdata + g)