package github.pdunn.gcp.examples.dataflow.datacatalog;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface DataCatalogExampleOpts extends DataflowPipelineOptions {

    @Description("e.g. bigquery-public-data:cmds_codes.hcpcs")
    @Validation.Required
    String getDestTable();
    void setDestTable(String destTable);

    @Description("e.g. projects/{prj_id}/locations/{region name}/tagTemplate/{template_name}")
    @Validation.Required
    String getTagTemplate();
    void setTagTemplate(String tagTemplate);

    @Description("column template")
    String getColumnTemplate();
    void setColumnTemplate(String columnTemplate);

    @Description("pub/sub subscription for streaming source")
    String getSubscription();
    void setSubscription(String subscription);
}
