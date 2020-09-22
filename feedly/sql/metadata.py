import simplejson as json

from .utils import METADATA

MODELS = METADATA.joinpath('models.json')
TABLES = METADATA.joinpath('tables.json')

with open(MODELS) as f:
    models = json.load(f)

with open(TABLES) as f:
    tables = json.load(f)
