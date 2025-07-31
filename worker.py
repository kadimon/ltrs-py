import logging

from hatchet_sdk import Hatchet, ClientConfig

import settings
from workflows.yandex_search import yandex_ltrs_workflow
from workflows.check_status import check_status
from workflows.topliba_com import topliba_com_workflow

root_logger = logging.getLogger('hatchet')
root_logger.setLevel(logging.WARNING)

hatchet = Hatchet(
    debug=False,
    config=ClientConfig(
        logger=root_logger,
    ),
)

def main():
    worker = hatchet.worker(
        name=f'scaper-{settings.SESSION}',
        slots=1,
        workflows=[
            yandex_ltrs_workflow,
            check_status,
            topliba_com_workflow,
        ],
    )

    worker.start()

if __name__ == '__main__':
    main()
