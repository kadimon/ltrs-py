import os
import interfaces

SESSION = os.environ['SESSION']
PROXY_URI = os.environ['PROXY_URI']
MONGO_URI = os.environ['MONGO_URI']
COVERS_DIR = os.environ['COVERS_DIR']

if labels_str := os.environ.get('LABELS'):
    labels_list = [
        set(l.split(':'))
        for l in labels_str.split(',')
        if l.strip()
    ]
    print(labels_list)
    WORKER_LABELS = interfaces.WorkerLabels(**{
        k.strip(): v.strip() for (k,v) in labels_list
    })
    print(WORKER_LABELS)
else:
    WORKER_LABELS = interfaces.WorkerLabels()


DEBUG = os.environ.get('DEBUG')
