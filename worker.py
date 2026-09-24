import importlib
import inspect
import pathlib
import pkgutil

from hatchet_sdk import (
    ConcurrencyExpression,
    ConcurrencyLimitStrategy,
    Context,
    Workflow,
)
from hatchet_sdk.labels import DesiredWorkerLabel

import settings
from settings import hatchet
from workflow_base import BaseWorkflow

WORKFLOWS_DIR = pathlib.Path(__file__).parent / 'workflows'
PACKAGE_NAME = 'workflows'  # папка должна содержать __init__.py


def create_task_for_class(wf: type[BaseWorkflow]) -> Workflow:
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
        # Что открыть — браузер или httpx-клиент — решает сам класс через `session()`
        async with wf.session() as session:
            return await wf.call_task(input, session, ctx=ctx)

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
