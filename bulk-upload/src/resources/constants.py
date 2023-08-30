from datetime import datetime

user = 'sys'
current_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

entity_columns = ['entity_id', 'entity_template_id', 'entity_name', 'entity_physical_name', 'primary_key']
entity_props_columns=['entity_prop_id', 'entity_id', 'prop_key', 'prop_value']


rule_columns = ['rule_id', 'ruleset_id', 'rule_template_id', 'rule_name', 'rule_description']


# parameters for loading df to DB
ruleset_db_columns = 'ruleset_id,ruleset_name,ruleset_desc,notification_preference,created_by,created_date,updated_by,updated_date'
ruleset_df_columns = ['ruleset_id', 'ruleset_name', 'ruleset_description', 'notification_preferences', 'created_by', 'created_date', 'updated_by', 'updated_date']

entity_db_columns = 'entity_id,entity_template_id,entity_name,entity_physical_name,entity_primary_key,created_by,created_date,updated_by,updated_date'
entity_df_columns = ['entity_id','entity_template_id','entity_name','entity_physical_name','primary_key','created_by','created_date','updated_by','updated_date']

entity_props_db_columns = 'entity_prop_id,entity_id,entity_prop_key,entity_prop_value,created_by,created_date,updated_by,updated_date'
entity_props_df_columns = ['entity_prop_id','entity_id','prop_key','prop_value','created_by','created_date','updated_by','updated_date']

rule_db_columns = 'rule_id,ruleset_id,rule_template_id,rule_name,rule_desc,created_by,created_date,updated_by,updated_date'
rule_df_columns = ['rule_id','ruleset_id','rule_template_id','rule_name','rule_description','created_by','created_date','updated_by','updated_date']

rule_props_db_columns = 'rule_prop_id,rule_id,rule_prop_key,rule_prop_value,created_by,created_date,updated_by,updated_date'
rule_props_df_columns = ['rule_prop_id','rule_id','prop_key','prop_value','created_by','created_date','updated_by','updated_date']

rule_entity_db_columns = 'rule_entity_map_id,rule_id,entity_id,entity_behaviour,is_primary,created_by,created_date,updated_by,updated_date'
rule_entity_df_columns = ['rule_entity_id','rule_id','entity_id','entity_behaviour','is_primary','created_by','created_date','updated_by','updated_date']


table_list = ['entity', 'entity_properties', 'entity_template', 'entity_template_properties', 'rule', 'rule_entity_map', 'rule_properties', 'rule_template', 'rule_template_properties', 'ruleset']