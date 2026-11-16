# Read all pipeline YAML files
locals {
  pipeline_files = var.pipelines_path != "" ? fileset(var.pipelines_path, "*.{yaml,yml}") : []
  pipeline_configs = {
    for f in local.pipeline_files :
    trimsuffix(trimsuffix(basename(f), ".yaml"), ".yml") => file("${var.pipelines_path}/${f}")
  }
}

# Write CONFIG rows to control table
resource "aws_dynamodb_table_item" "pipeline_config" {
  for_each   = local.pipeline_configs
  table_name = aws_dynamodb_table.control.name
  hash_key   = "PK"
  range_key  = "SK"

  item = jsonencode({
    PK     = { S = "PIPELINE#${each.key}" }
    SK     = { S = "CONFIG" }
    config = { S = jsonencode(yamldecode(each.value)) }
  })

  # The Lambdas read the pipeline ID from the PK, so a file named for a
  # different ID would be rejected at load. Fail here first.
  lifecycle {
    precondition {
      condition     = try(tostring(yamldecode(each.value).pipeline.id), null) == each.key
      error_message = "Pipeline file ${each.key}: pipeline.id must equal the file name without .yaml or .yml."
    }
  }
}
