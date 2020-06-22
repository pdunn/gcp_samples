import argparse
import logging
import datetime
import re

import apache_beam as beam
import apache_beam.io.gcp.bigquery_tools as bqt
from google.cloud import datacatalog_v1


def run(argv=None):
    #  bigquery-public-data:geo_openstreetmap.planet_relations
    dest_table_arg = 'billing-test-271515:master_tables.stuff'
    template_id = 'projects/billing-test-271515/locations/us-central1/tagTemplates/dataflow_meta' 
    
    dest_table_ref = bqt.parse_table_reference(dest_table_arg)
    dest_pid = dest_table_ref.projectId
    dest_did = dest_table_ref.datasetId
    dest_tid = dest_table_ref.tableId
    
    table_resource_id = 'bigquery.table.`{}`.{}.{}'\
        .format(dest_pid, dest_did, dest_tid)

    
    time = str(datetime.datetime.utcnow()).split('.')[0]

    with beam.Pipeline() as pipeline:
        words = (
            pipeline
            | 'create_samples' >> beam.Create([
                {'val':'sample_1', 'update_dt': time},
                {'val':'sample_2', 'update_dt': time},
                {'val':'sample_2', 'update_dt': time}
            ])
            | 'update_catalog' >> UpdateCatalog(table_resource_id, template_id))


class UpdateCatalog(beam.PTransform):
    """
        Combine down the data and write to data catalog
        table_id = "bigquery.table.`{project-id}`.{dataset}.{table}
        template_id = "projects/project-id/locations/region/tagTemplates/template_name
    
    """
    def __init__(self, table_res_id, template_id):
        self.table_id = table_res_id
        self.template_id = template_id

    def write_to_catalog(self, count):
        datacatalog = datacatalog_v1.DataCatalogClient()
       
        table_entry = datacatalog.lookup_entry(sql_resource=self.table_id)
        tags = datacatalog.list_tags(parent=table_entry.name)

        tag = [t for t in tags if t.template == self.template_id]

        if len(tag) > 0:
            nt = datacatalog_v1.types.Tag()
            nt.name = tag[0].name
            nt.template = self.template_id
            nt.fields['build_id'].string_value = 'count_{}'.format(count)
            datacatalog.update_tag(nt)
        else:
            nt = datacatalog_v1.types.Tag()
            nt.template = self.template_id
            nt.fields['build_id'].string_value = 'count_{}'.format(count)
            datacatalog.create_tag(table_entry.name, nt)
            
    def expand(self, elements):
        (elements
        | 'reduce_window' >> beam.CombineGlobally(beam.combiners.CountCombineFn())
        | beam.Map(self.write_to_catalog))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    print('end')



