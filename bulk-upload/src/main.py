import pandas as pd
import random
import string
import sys
import os
sys.path.append('/'.join(os.path.abspath(__file__).split('\\')[:-1]) + '/resources')
from util import *
from resources.util import *
from resources.build_ruleset import BuildRuleset
from resources.build_entity import BuildEntity
from resources.build_rules import BuildRules
from resources.map_templates import MapTemplates


ruleset_df = read_file('bulk-upload/data_files/Bulk_upload_template_HSBC.xlsx', 'ruleset')
ruleset_df = rename_columns(ruleset_df)
build_ruleset = BuildRuleset(ruleset_df)
ruleset_mapper = build_ruleset.build()

entity_df = read_file('bulk-upload/data_files/Bulk_upload_template_HSBC.xlsx', 'entity')
entity_df = rename_columns(entity_df)
build_entity = BuildEntity(entity_df)
entity_mapper, entity_df = build_entity.build()

rule_df = read_file('bulk-upload/data_files/Bulk_upload_template_HSBC.xlsx', 'rules')
rule_df = rename_columns(rule_df)
build_rules = BuildRules()
rule_df = build_rules.build(rule_df, entity_mapper, ruleset_mapper)
