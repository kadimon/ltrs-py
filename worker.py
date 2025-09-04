import importlib
import inspect
import pkgutil
import pathlib
import logging

from hatchet_sdk import (
    Hatchet,
    ClientConfig,
    Context,
    ConcurrencyExpression,
    ConcurrencyLimitStrategy,
    Workflow
)
from playwright.async_api import async_playwright

from workflow_base import BaseLitresPartnersWorkflow
import settings


root_logger = logging.getLogger('hatchet')
root_logger.setLevel(logging.WARNING)

hatchet = Hatchet(
    debug=False,
    config=ClientConfig(
        logger=root_logger,
    ),
)

WORKFLOWS_DIR = pathlib.Path(__file__).parent / 'workflows'
PACKAGE_NAME = 'workflows'  # папка должна содержать __init__.py


def create_task_for_class(wf: BaseLitresPartnersWorkflow) -> Workflow:

    async def task_function(input: wf.input, ctx: Context) -> wf.output:

        async with async_playwright() as p:
            context = await p.firefox.launch_persistent_context(
                './profileDir',
                proxy={'server': settings.PROXY_URI} if wf.proxy_enable else None,
                headless=False,
                viewport={'width': 1920, 'height': 1080},
                timeout=10_000,
            )

            page = context.pages[0] if context.pages else await context.new_page()
            instance = wf(
                name=wf.name,
                event=wf.event,
                customer=wf.customer,
                input=wf.input,
                output=wf.output,
            )
            result = await instance.task(input, page)

            await context.close()

            return result

    task = hatchet.task(
        name=wf.name,
        on_events=[wf.event],
        input_validator=wf.input,
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

    )(task_function)

    return task


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
        workflows=workflows,
    )
    worker.start()


if __name__ == '__main__':
    main()
