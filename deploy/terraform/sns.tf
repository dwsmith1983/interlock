resource "aws_sns_topic" "alerts" {
  name = "${var.table_name}-alerts"
}
