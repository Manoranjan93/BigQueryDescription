from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
import json
import yaml
import os

class BigQueryTableUpdater:
    def __init__(self, yaml_config_path):
        self.storage_client = storage.Client()
        self.bigquery_client = bigquery.Client()
        self.yaml_config_path = yaml_config_path
        self.table_configs = self.load_yaml_config()

    def load_yaml_config(self):
        with open(self.yaml_config_path, 'r') as file:
            return yaml.safe_load(file)['tables']

    def update_field_descriptions(self, fields, updates):
        updated_fields = []
        for field in fields:
            new_desc = updates.get(field.name, {}).get('description', field.description)
            if field.field_type == 'RECORD' and 'fields' in updates.get(field.name, {}):
                nested_updates = {f['name']: f for f in updates[field.name]['fields']}
                subfields = self.update_field_descriptions(field.fields, nested_updates)
                updated_field = bigquery.SchemaField(name=field.name, 
                                                     field_type=field.field_type, 
                                                     mode=field.mode, 
                                                     description=new_desc, 
                                                     fields=subfields)
            else:
                updated_field = bigquery.SchemaField(name=field.name, 
                                                     field_type=field.field_type, 
                                                     mode=field.mode, 
                                                     description=new_desc)
            updated_fields.append(updated_field)
        return updated_fields

    def update_table_descriptions(self):
        for config in self.table_configs:
            gcs_path = config['gcs_json_path']
            bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
            local_file_name = blob_name.split('/')[-1]

            try:
                bucket = self.storage_client.bucket(bucket_name)
                blob = bucket.blob(blob_name)
                blob.download_to_filename(local_file_name)

                with open(local_file_name, 'r') as file:
                    description_updates = json.load(file)

                new_table_description = description_updates.get('description', '')
                column_updates = {field['name']: field for field in description_updates.get('schema', {}).get('fields', [])}

                full_table_id = f"{config['project_id']}.{config['dataset_id']}.{config['table_id']}"
                table = self.bigquery_client.get_table(full_table_id)

                if new_table_description:
                    table.description = new_table_description

                table.schema = self.update_field_descriptions(table.schema, column_updates)
                self.bigquery_client.update_table(table, ['description', 'schema'])

                print(f"Updated descriptions for table {full_table_id}")

            except NotFound:
                print(f"Table {full_table_id} not found in BigQuery. Skipping.")
                continue
            finally:
                if os.path.exists(local_file_name):
                    os.remove(local_file_name)

if __name__ == "__main__":
    updater = BigQueryTableUpdater('table_configs.yaml')
    updater.update_table_descriptions()
