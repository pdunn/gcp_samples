package github.pdunn.gcp.examples.dataflow.datacatalog;


import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;


public class CatalogUpdateBatchJob {
    private static final Logger LOG =
            LoggerFactory.getLogger(CatalogUpdateBatchJob.class);

    public void execute(DataCatalogExampleOpts opts) {
        TableReference tRef = BigQueryHelpers.parseTableSpec(opts.getDestTable());
        String sqlRef = String.format("bigquery.table.`%s`.%s.%s",
                tRef.getProjectId(), tRef.getDatasetId(), tRef.getTableId());

        Pipeline p = Pipeline.create(opts);

        PCollection<TableRow> rows = p.apply(
                Create.of(tr("lemon"), tr("strawberry"), tr("mango")));

        WriteResult wr = rows.apply("write_to_bq", BigQueryIO.writeTableRows()
                .to(tRef)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        //can optionally be joined back to rows to get more accurate counts
        PCollection<TableRow> insertRows = wr.getFailedInserts();

        rows.apply(new UpdateCatalog(
                opts.getJobName(),
                sqlRef,
                opts.getTagTemplate(),
                opts.getColumnTemplate(),
                Collections.singletonMap("value", "beam_java_create")));

        PipelineResult res = p.run();
        res.waitUntilFinish();
    }


    //code assumes schema of {value:STRING, update_dt: DATETIME}
    private static TableRow tr(String value) {
        TableRow tr = new TableRow();
        tr.set("value", value);
        tr.set("update_dt", new LocalDateTime().toString());
        return tr;
    }

    public static void main(String[] args) throws Exception {
        LOG.info("command line args " + Arrays.asList(args));

        PipelineOptionsFactory.register(DataCatalogExampleOpts.class);
        DataCatalogExampleOpts opts = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DataCatalogExampleOpts.class);

        CatalogUpdateBatchJob job = new CatalogUpdateBatchJob();
        job.execute(opts);
    }
}
