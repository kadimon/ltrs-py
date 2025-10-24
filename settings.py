import os
import interfaces
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

SESSION = os.environ['SESSION']
PROXY_URI = os.environ['PROXY_URI']
MONGO_URI = os.environ['MONGO_URI']

AWS_ENDPOINT_URL = os.environ['AWS_ENDPOINT_URL']
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_COVERS_BUCKET = os.environ['AWS_COVERS_BUCKET']
AWS_COVERS_DIR = 'covers'


if labels_str := os.environ.get('LABELS'):
    labels_list = [
        l.split(':')
        for l in labels_str.split(',')
        if l.strip()
    ]
    WORKER_LABELS = interfaces.WorkerLabels(**{
        k.strip(): v.strip() for (k,v) in labels_list
    })
else:
    WORKER_LABELS = interfaces.WorkerLabels()


DEBUG = os.environ.get('DEBUG')
DEBUG_PW_SERVER = 'ws://127.0.0.1:3000/'
RUN = os.environ.get('RUN')

START_TIME = datetime.now().strftime('%Y%m%d%H%M%S')
