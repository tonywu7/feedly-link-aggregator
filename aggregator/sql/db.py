from pathlib import Path

import simplejson as json

from .factory import Database

with open(Path(__file__).with_name('db.json')) as f:
    db = Database(json.load(f))
