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


def create_task_for_class(cls: BaseLitresPartnersWorkflow) -> Workflow:

    async def task_function(input: cls.input, ctx: Context) -> cls.output:

        async with async_playwright() as p:
            context = await p.firefox.launch_persistent_context(
                './profileDir',
                proxy={'server': settings.PROXY_URI},
                headless=False,
                viewport={'width': 1920, 'height': 1080},
            )

            page = context.pages[0] if context.pages else await context.new_page()
            instance = cls()
            result = await instance.task(input, ctx, page)

            await context.close()

            return result

    task = hatchet.task(
        name=cls.name,
        on_events=[cls.event],
        input_validator=cls.input,
        concurrency=ConcurrencyExpression(
            expression=f"'{cls.name}'",
            max_runs=cls.concurrency,
            limit_strategy=ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        ),
        execution_timeout=f'{cls.execution_timeout_sec}s',
        schedule_timeout=f'{cls.schedule_timeout_hours}h',
        retries=cls.retries,
        backoff_max_seconds=cls.backoff_max_seconds,
        backoff_factor=cls.backoff_factor,

    )(task_function)

    return task


def load_workflows() -> list[Workflow]:
    workflows = []

    for module_info in pkgutil.iter_modules([str(WORKFLOWS_DIR)]):
        module_name = f'{PACKAGE_NAME}.{module_info.name}'
        module = importlib.import_module(module_name)

        classes = [
            obj
            for _, obj in inspect.getmembers(module, inspect.isclass)
            if obj.__module__ == module_name
        ]

        for cls in classes:
            # Создаём таск для этого класса
            workflows.append(create_task_for_class(cls))

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
