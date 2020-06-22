import argparse
import datetime
import time
import logging
import re

import apache_beam as beam
import apache_beam.io.gcp.bigquery_tools as bqt
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import datacatalog_v1


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--dest_table', dest='dest_table', required=True)
    parser.add_argument('--tag_template', dest='tag_template', required=True)
    parser.add_argument('--col_template', dest='col_template')
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
        
        vals | 'write_to_bq' >> beam.io.WriteToBigQuery(
            dest_table_ref,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)


        table_resource_id = 'bigquery.table.`{}`.{}.{}'\
            .format(dest_table_ref.projectId,
             dest_table_ref.datasetId,
            dest_table_ref.tableId)

        vals | UpdateCatalog(table_resource_id, 
                            known_args.tag_template, 
                            known_args.col_template,
                            [('value', 'beam_create')])



class UpdateCatalog(beam.PTransform):
    """
        table_res_id = sql_refence in lookup_entry method
        table_template_id = template identifier of the tag on the table, e.g. projects/{}/locations/us-central1/tagTemplates/{}
        col_template_id = template identifier of the tag on the columns
        src_map = list of tuples src(0) = column tag is added to, src(1) = value attached to the "source" field
    """
    def __init__(self, table_res_id, table_template_id, col_template_id=None, src_map=[]):
        self.table_id = table_res_id
        self.table_template_id = table_template_id
        self.col_template_id = col_template_id
        self.src_map = src_map

    def create_tags(self, count):
        nt = datacatalog_v1.types.Tag()
        nt.template = self.table_template_id
        nt.fields['job_name'].string_value = 'bq_catalog_example'
        nt.fields['timestamp'].timestamp_value.seconds = int(time.time())
        nt.fields['update_count'].double_value = count

        tags = [nt]
        if self.col_template_id:
            for src in self.src_map:
                col_tag = datacatalog_v1.types.Tag()
                col_tag.template = self.col_template_id
                col_tag.column = src[0]
                col_tag.fields['source'].string_value = src[1]

                tags.append(col_tag)
                
        return tags

    def write_to_catalog(self, new_tag_list):
        print('write to catalog')
        datacatalog = datacatalog_v1.DataCatalogClient()

        table_entry = datacatalog.lookup_entry(sql_resource=self.table_id)
        tags = datacatalog.list_tags(parent=table_entry.name)

        tag_index = {}
        for t in tags:
            column = t.column if len(t.column) > 0 else '_table'
            tag_templates = tag_index.setdefault(t.template, {})
            tag_templates[column] = t 
        
        for nt in new_tag_list:
            ckey = nt.column if len(nt.column) > 0 else '_table'
            old_tag = tag_index.get(nt.template, {}).get(ckey)

            if old_tag:
                nt.name = old_tag.name
                datacatalog.update_tag(nt)
            else:
                datacatalog.create_tag(table_entry.name, nt)
            
    def expand(self, elements):
        (elements
        | 'reduce_window' >> beam.CombineGlobally(beam.combiners.CountCombineFn())
        | beam.Map(self.create_tags)
        | beam.Map(self.write_to_catalog))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    print('end')



