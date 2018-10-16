import apache_beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
from apache_beam.io.textio import ReadFromText, WriteToText

import re
import logging
from google.cloud import storage

PROJECT_ID = '<GCP-PROJECT-ID>'

#RUNNER = 'DataflowRunner'
#INPUT_FILEPATH= 'gs://{}/flights/trainday.csv'.format(PROJECT_ID)
#OUTPUT_FILEPATH= 'gs://{}/output/out.txt'.format(PROJECT_ID)
#SUMMARY_FILEPATH= 'gs://{}/output/summary.txt'.format(PROJECT_ID)
#OUTPUT_TEMP_FILEPATH= 'gs://{}/output/temp'.format(PROJECT_ID)

RUNNER = 'DirectRunner'
INPUT_FILEPATH= 'trainday.csv'
OUTPUT_FILEPATH = 'output.txt'
SUMMARY_FILEPATH = 'summary.txt'
OUTPUT_TEMP_FILEPATH = './temp/'
START_WORKERS = 1
MAX_WORKERS = 8

header = 'FL_DATE,is_train_day'

class FilterHeader(apache_beam.DoFn):
    def __init__(self, header):
        self.__header = header

    def process(self, element):
        if element != self.__header:
            yield element

class Parse(apache_beam.DoFn):
    def process(self, element):
        data = element.split(",")
        if len(data) == 2:
            yield {
                    'Result': True,
                    'Data': data
                  }

        else:
            yield {
                    'Result': False,
                    'Data': element
                  }

class FilterTrainingDays(apache_beam.DoFn):
    def process(self, element):
        if element['Result']:
            data = element['Data']
            if data[1].lower() in ['true', 'yes']:
                yield 1
            else:
                yield 0

class Summary(apache_beam.DoFn):
    def process(self, element):
        if element['Result']:
            yield u'OK'
        else:
            msg = u'Error at: {}'.format(element['Data'])
            import logging
            logging.basicConfig(level=logging.INFO)
            logging.getLogger('dataflow_poc').warning(msg)
            yield msg

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('dataflow_poc')

    input_file = INPUT_FILEPATH
    summary_file = SUMMARY_FILEPATH
    output_file = OUTPUT_FILEPATH

    options = PipelineOptions()
    gcloud_options = options.view_as(GoogleCloudOptions)
    worker_options = options.view_as(WorkerOptions)
    gcloud_options.project = PROJECT_ID
    gcloud_options.temp_location = OUTPUT_TEMP_FILEPATH
    worker_options.num_workers = START_WORKERS
    worker_options.max_num_workers = MAX_WORKERS
    gcloud_options.job_name = 'csv-transform'

    options.view_as(StandardOptions).runner = RUNNER
    logger.info('Ready to load the file')
    with apache_beam.Pipeline(options=options) as pipe:
        datarows = pipe | ReadFromText(input_file) | apache_beam.ParDo(FilterHeader(header)) | apache_beam.ParDo(Parse())
        datarows | apache_beam.ParDo(Summary()) | "WriteSummary" >>  WriteToText(summary_file)
        datarows | apache_beam.ParDo(FilterTrainingDays()) | apache_beam.CombineGlobally(sum) |  "WriteCount" >> WriteToText(output_file)


if __name__ == '__main__':
    main()

