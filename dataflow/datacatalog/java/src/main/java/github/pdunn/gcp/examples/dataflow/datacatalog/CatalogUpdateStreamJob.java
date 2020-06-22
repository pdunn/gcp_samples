package github.pdunn.gcp.examples.dataflow.datacatalog;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

public class CatalogUpdateStreamJob {
    public static final Logger LOG =
            LoggerFactory.getLogger(CatalogUpdateBatchJob.class);

    public void execute(DataCatalogExampleOpts opts) {
        TableReference tRef = BigQueryHelpers.parseTableSpec(opts.getDestTable());
        String sqlRef = String.format("bigquery.table.`%s`.%s.%s",
                tRef.getProjectId(), tRef.getDatasetId(), tRef.getTableId());

        FixedWindows window = FixedWindows.of(Duration.standardMinutes(5));
        Pipeline p = Pipeline.create(opts);

        //Create a window that applies uniformly to the BQ insert
        // and the Data Catalog Update
        PCollection<TableRow> rows = p
                .apply(PubsubIO.readStrings()
                        .fromSubscription(opts.getSubscription()))
                .apply("convert_to_bq_row",ParDo.of(messageToRow()))
                .apply("fixed_window",
                        Window.<TableRow>into(window));

        WriteResult wr = rows.apply("write_to_bq", BigQueryIO.writeTableRows()
                .to(tRef)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        //can optionally be joined back to rows to get more accurate counts
        PCollection<TableRow> insertErrorCount = wr.getFailedInserts();

        rows.apply(new UpdateCatalog(
                opts.getJobName(),
                sqlRef,
                opts.getTagTemplate(),
                opts.getColumnTemplate(),
                Collections.singletonMap("value", "beam_java_create")));

        //NOTE: will run indefinitely for 60 minutes unless stopped earlier
        PipelineResult res = p.run();
        res.waitUntilFinish(Duration.standardMinutes(60));
    }

    private static DoFn<String, TableRow> messageToRow() {
        return new DoFn<String, TableRow>() {
            @ProcessElement
            public void process(@Element String input, OutputReceiver<TableRow> out, ProcessContext c) {
                String udt = c.timestamp().toDateTime().toLocalDateTime().toString();

                TableRow tr = new TableRow();
                tr.set("value", input);
                tr.set("update_dt", udt);

                out.output(tr);
            }
        };
    }

    public static void main(String[] args) throws Exception {
        LOG.info("command line args " + Arrays.asList(args));

        PipelineOptionsFactory.register(DataCatalogExampleOpts.class);
        DataCatalogExampleOpts opts = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DataCatalogExampleOpts.class);

        CatalogUpdateStreamJob job = new CatalogUpdateStreamJob();
        job.execute(opts);
    }
}
