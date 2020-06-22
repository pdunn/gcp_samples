package github.pdunn.gcp.examples.dataflow.datacatalog;


import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.datacatalog.v1.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class UpdateCatalog extends PTransform<PCollection<TableRow>, PCollection<Long>> {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateCatalog.class);

    private String jobName;
    private String sqlRefId;
    private String tabTempId;
    private String colTempId;
    private Map<String,String> fieldToSourceVal;
    private PCollectionView<Long> bqResultSideInput;

    public UpdateCatalog(String jobName,
                         String sqlRefId,
                         String tabTempId,
                         String colTempId,
                         Map<String, String> fieldToSourceVal) {

        this.jobName = jobName;
        this.sqlRefId = sqlRefId;
        this.tabTempId = tabTempId;
        this.colTempId = colTempId;
        this.fieldToSourceVal = fieldToSourceVal;
    }

    private class CreateTags extends DoFn<Long, List<Tag>> {
        @ProcessElement
        public void process(@Element Long count, OutputReceiver<List<Tag>> out, ProcessContext c) {
            long errorCount = c.sideInput(bqResultSideInput);
            long seconds = new DateTime().getMillis() / 1000;

            List<Tag> tags = new ArrayList<>();
            Tag tableTag = Tag.newBuilder()
                    .setTemplate(tabTempId)
                    .putFields("job_name", TagField.newBuilder().setStringValue(jobName).build())
                    .putFields("timestamp",
                            TagField.newBuilder()
                                    .setTimestampValue(com.google.protobuf.Timestamp.newBuilder()
                                            .setSeconds(seconds)
                                            .build())
                                    .build())
                    .putFields("update_count", TagField.newBuilder()
                            .setDoubleValue(count - errorCount)
                            .build())
                    .build();
            tags.add(tableTag);

            if (colTempId != null && fieldToSourceVal != null) {
                for (Map.Entry<String, String> map : fieldToSourceVal.entrySet()) {
                    Tag colTag = Tag.newBuilder()
                            .setTemplate(colTempId)
                            .setColumn(map.getKey())
                            .putFields("source",
                                    TagField.newBuilder().setStringValue(map.getValue()).build())
                            .build();
                    tags.add(colTag);
                }
            }
            out.output(tags);
        }
    }

    private class WriteToCatalog extends DoFn<List<Tag>, Long> {

        private static final String TABLE_KEY = "_table";

        @ProcessElement
        public void process(@Element List<Tag> tags, OutputReceiver<Long> out) {
            long count = 0;
            try (DataCatalogClient client = DataCatalogClient.create()) {
                Entry entry = client.lookupEntry(
                        LookupEntryRequest.newBuilder()
                                .setSqlResource(sqlRefId).build());
                Map<String,Map<String,Tag>> index = createTagIndex(client, entry);

                for (Tag nt : tags) {
                    String key = nt.getColumn() == null || nt.getColumn().length() == 0 ? TABLE_KEY : nt.getColumn();
                    Tag ot = index.getOrDefault(nt.getTemplate(), new HashMap<>()).get(key);

                    if (ot != null) {
                        Tag update = nt.toBuilder().setName(ot.getName()).build();
                        client.updateTag(update);
                    } else {
                        client.createTag(entry.getName(), nt);
                    }
                    count += 1;
                }

            } catch (IOException ioe) {
                LOG.error("Unable to create Data Catalog client");
            }
            out.output(count);
        }

        private Map<String,Map<String, Tag>> createTagIndex(DataCatalogClient client, Entry entry) {

            DataCatalogClient.ListTagsPagedResponse resp = client.listTags(entry.getName());

            Map<String,Map<String,Tag>> index = new HashMap<>();
            for (Tag t : resp.iterateAll()) {
                index.putIfAbsent(t.getTemplate(), new HashMap<>());
                Map<String, Tag> templateCols = index.get(t.getTemplate());
                String key = t.getColumn() == null || t.getColumn().length() == 0 ? TABLE_KEY : t.getColumn();
                templateCols.put(key, t);
            }
            return index;
        }
    }

    @Override
    public PCollection<Long> expand(PCollection<TableRow> input) {
        return input.apply(Combine.globally(Count.<TableRow>combineFn()).withoutDefaults())
                .apply(ParDo.of(new CreateTags()).withSideInputs(bqResultSideInput))
                .apply(ParDo.of(new WriteToCatalog()));
    }

}