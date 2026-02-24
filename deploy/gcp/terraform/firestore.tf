# -----------------------------------------------------------------------------
# Firestore database (Native mode)
# -----------------------------------------------------------------------------

resource "google_firestore_database" "main" {
  count       = var.create_firestore_database ? 1 : 0
  name        = var.firestore_database
  location_id = var.region
  type        = "FIRESTORE_NATIVE"

  delete_protection_state = var.destroy_on_delete ? "DELETE_PROTECTION_DISABLED" : "DELETE_PROTECTION_ENABLED"
}

# -----------------------------------------------------------------------------
# TTL policy on the 'ttl' timestamp field
# -----------------------------------------------------------------------------

resource "google_firestore_field" "ttl" {
  project    = var.project_id
  database   = var.firestore_database
  collection = var.collection_name
  field      = "ttl"

  ttl_config {}

  # Disable single-field indexing for the TTL field (not queried directly)
  index_config {}

  depends_on = [google_firestore_database.main]
}

# -----------------------------------------------------------------------------
# Composite index for GSI1-equivalent queries (gsi1pk + gsi1sk)
# Used for: ListPipelines, ListAllReruns, ListReplays
# -----------------------------------------------------------------------------

resource "google_firestore_index" "gsi1" {
  project    = var.project_id
  database   = var.firestore_database
  collection = var.collection_name

  fields {
    field_path = "gsi1pk"
    order      = "ASCENDING"
  }

  fields {
    field_path = "gsi1sk"
    order      = "ASCENDING"
  }

  depends_on = [google_firestore_database.main]
}
