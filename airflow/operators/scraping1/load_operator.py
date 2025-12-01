from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from scripts.scraping1.loader_runner import run_loader

class LoadOperator(BaseOperator):

    @apply_defaults
    def __init__(self, config_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config_path=config_path

    def execute(self, context):
        run_loader(self.config_path)
