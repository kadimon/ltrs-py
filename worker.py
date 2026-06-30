import importlib
import inspect
import pathlib
import pkgutil
from pathlib import Path

from camoufox.async_api import AsyncCamoufox
from hatchet_sdk import (
    ConcurrencyExpression,
    ConcurrencyLimitStrategy,
    Context,
    Workflow,
)
from hatchet_sdk.labels import DesiredWorkerLabel

import settings
from settings import hatchet
from workflow_base import BaseLitresPartnersWorkflow

WORKFLOWS_DIR = pathlib.Path(__file__).parent / 'workflows'
PACKAGE_NAME = 'workflows'  # папка должна содержать __init__.py


def create_task_for_class(wf: BaseLitresPartnersWorkflow) -> Workflow:
    @hatchet.task(
        name=wf.name,
        on_events=[wf.event],
        input_validator=wf.input,
        desired_worker_labels={
            k: DesiredWorkerLabel(
                value=v,
                required=True,
                # comparator=WorkerLabelComparator.EQUAL,
                # weight=10,
            )
            for k, v in wf.labels.items()
        },
        concurrency=ConcurrencyExpression(
            expression=f"'{wf.name}'",
            max_runs=wf.concurrency,
            limit_strategy=ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        ),
        execution_timeout=f'{wf.execution_timeout_sec}s',
        schedule_timeout=f'{wf.schedule_timeout_hours}h',
        retries=wf.retries,
        backoff_max_seconds=wf.backoff_max_seconds,
        backoff_factor=wf.backoff_factor,

    )
    async def task_function(input: wf.input, ctx: Context) -> wf.output:
        addons_dir = Path(settings.BROWSER_ADDONS_DIR)
        if addons_dir.exists():
            addons_paths_list = [str(f.resolve()) for f in addons_dir.iterdir() if addons_dir.is_dir()]
        else:
            addons_paths_list = []

        async with AsyncCamoufox(
            os='windows',
            humanize=True,
            headless='virtual',
            persistent_context=True,
            user_data_dir='user_data',
            locale=['ru-RU', 'en-US'],
            addons=addons_paths_list,
            proxy={
                'server': settings.PROXY_URI if wf.proxy_enable else None,
            }
        ) as browser:
            page = await browser.new_page()

            instance = wf(
                name=wf.name,
                event=wf.event,
                customer=wf.customer,
                input=wf.input,
                output=wf.output,
            )
            result = await instance.task(input, page)

            return result

    return task_function


def load_workflows() -> list[Workflow]:
    workflows = []

    for module_info in pkgutil.iter_modules([str(WORKFLOWS_DIR)]):
        module_name = f'{PACKAGE_NAME}.{module_info.name}'
        module = importlib.import_module(module_name)

        classes_wf = [
            obj
            for _, obj in inspect.getmembers(module, inspect.isclass)
            if obj.__module__ == module_name
        ]

        for wf in classes_wf:
            # Создаём таск для этого класса
            workflows.append(create_task_for_class(wf))

    return workflows


def main() -> None:
    workflows = load_workflows()

    worker = hatchet.worker(
        name=f'scaper-{settings.SESSION}',
        slots=1,
        labels=settings.WORKER_LABELS,
        workflows=workflows,
    )
    worker.start()


if __name__ == '__main__':
    main()
