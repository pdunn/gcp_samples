import argparse
import datetime
import time
import logging
import re

import apache_beam as beam
import apache_beam.io.gcp.bigquery_tools as bqt
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from google.cloud import datacatalog_v1


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--dest_table', dest='dest_table', required=True)
    parser.add_argument('--tag_template', dest='tag_template', required=True)
    parser.add_argument('--job_name', dest='job_name', default='bigquery_meta_sample')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    dest_table_ref = bqt.parse_table_reference(known_args.dest_table)
    
    ts = str(datetime.datetime.utcnow()).split('.')[0]

    with beam.Pipeline(options=pipeline_options) as pipeline:
        vals = (
            pipeline
            | 'create_samples' >> beam.Create([
                {'value':'strawberry', 'update_dt': ts},
                {'value':'lemon', 'update_dt': ts},
                {'value':'mango', 'update_dt': ts}
            ]))
        
        (vals | 'write_to_bq' >> beam.io.WriteToBigQuery(
            dest_table_ref,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))

        table_resource_id = 'bigquery.table.`{}`.{}.{}'\
            .format(dest_table_ref.projectId, 
            dest_table_ref.datasetId, 
            dest_table_ref.tableId)

        catalog = UpdateCatalog(known_args.job_name, table_resource_id, known_args.tag_template)


        vals | catalog



class UpdateCatalog(beam.PTransform):
    def __init__(self, job_name, table_res_id, table_template_id):
        self.job_name = job_name
        self.table_id = table_res_id
        self.table_template_id = table_template_id

    def create_tag(self, count):
        nt = datacatalog_v1.types.Tag()
        nt.template = self.table_template_id
        nt.fields['job_name'].string_value = self.job_name
        nt.fields['timestamp'].timestamp_value.seconds = int(time.time())
        nt.fields['update_count'].double_value = count

        return nt

    def write_to_catalog(self, new_tag):
        print('write to catalog')
        datacatalog = datacatalog_v1.DataCatalogClient()

        table_entry = datacatalog.lookup_entry(sql_resource=self.table_id)
        tags = datacatalog.list_tags(parent=table_entry.name)

        tag_list = [t for t in tags if (t.template == new_tag.template and len(t.column) == 0)]

        if len(tag_list) > 0:
            new_tag.name = tag_list[0].name
            datacatalog.update_tag(new_tag)
        else:
            datacatalog.create_tag(table_entry.name, new_tag)
            
    def expand(self, elements):
        (elements
        | 'reduce_window' >> beam.CombineGlobally(beam.combiners.CountCombineFn())
        | beam.Map(self.create_tag)
        | beam.Map(self.write_to_catalog))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    print('end')



