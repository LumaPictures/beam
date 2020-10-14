"""
Basic graph to test using TaskWorker.
"""
# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import DirectOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import apache_beam as beam

from apache_beam.runners.worker.task_worker.core import BeamTask


class TestFn(beam.DoFn):

    def process(self, element, side):
        from apache_beam.runners.worker.task_worker import TaskableValue

        for s in side:
            if isinstance(element, TaskableValue):
                value = element.value
            else:
                value = element
            print(value + s)
            yield value + s


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the test pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(DirectOptions).direct_running_mode = 'multi_processing'

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as pipe:

        A = (
            pipe
            | 'A' >> beam.Create(range(3))
        )

        B = (
            pipe
            | beam.Create(range(2))
            | BeamTask(beam.ParDo(TestFn(), beam.pvalue.AsList(A)),
                       wrapper=lambda x, _: x)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
