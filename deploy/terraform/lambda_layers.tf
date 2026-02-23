data "archive_file" "archetype_layer" {
  type        = "zip"
  source_dir  = var.layer_dist_dir
  output_path = "${path.module}/.build/archetype-layer.zip"
}

resource "aws_lambda_layer_version" "archetypes" {
  layer_name               = "${var.table_name}-archetypes"
  filename                 = data.archive_file.archetype_layer.output_path
  source_code_hash         = data.archive_file.archetype_layer.output_base64sha256
  compatible_runtimes      = ["provided.al2023"]
  compatible_architectures = ["arm64"]
  description              = "Interlock archetype definitions"
}
