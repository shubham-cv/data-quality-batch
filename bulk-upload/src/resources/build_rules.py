import pandas as pd
import random
import string
from util import *
from build_ruleset import BuildRuleset
from build_entity import BuildEntity
from resources.map_templates import MapTemplates
from constants import *
import numpy as np


class BuildRules():
    def __init__(self):
        pass

    def build_rule_entity_map(self, rule_df):
        print(rule_df[['source_entity_id', 'target_entity_id', 'source_secondary_entity_ids', 'target_secondary_entity_ids']])
        rule_entity_df = pd.DataFrame(columns= ['rule_entity_id', 'rule_id', 'entity_id', 'entity_behaviour', 'is_primary'])
        for index, row in rule_df.iterrows():
            for i in ['source', 'target']:
                if not pd.isna(row[i+'_entity_id']):
                    rule_entity_df.loc[len(rule_entity_df)] = [get_unique_id(), row['rule_id'],
                                                                row[i+'_entity_id'], i, 'True']
            for i in ['source', 'target']:
                if not pd.isna(row[i+'_secondary_entity_ids']):
                    entities_split= row[i+'_secondary_entity_ids'].split(',')
                    for j in entities_split:
                        rule_entity_df.loc[len(rule_entity_df)] = [get_unique_id(), row['rule_id'],
                                                                    j.strip(), i, 'False']
                # else:
                #     continue
        
        # new_ids = get_new_ids(rule_entity_df)
        # rule_entity_df['rule_entity_id'] = new_ids
        rule_entity_df = add_who_columns(rule_entity_df)
        insert_into_db('rule_entity_map', rule_entity_db_columns, rule_entity_df, rule_entity_df_columns)
        rule_entity_df.to_csv('output/rule_entity_map.csv', index= False)

    def map_secondary_entity(self, rule_df, entity_mapper):
        rule_df['source_secondary_entity_ids'] = np.NaN
        rule_df['target_secondary_entity_ids'] = np.NaN
        for index, row in rule_df.iterrows():
            source_secondary_entity = row['source_secondary_entity']
            target_secondary_entity = row['target_secondary_entity']
            if not pd.isna(source_secondary_entity):
                if ',' in source_secondary_entity:
                    source_secondary_entity = row['source_secondary_entity'].split(',')
                    source_secondary_entity_ids = [entity_mapper[x.strip()] for x in source_secondary_entity]
                    source_secondary_entity_ids_string = ', '.join(source_secondary_entity_ids)
                    rule_df.loc[index, 'source_secondary_entity_ids'] = source_secondary_entity_ids_string
                else:
                    source_secondary_entity_id = entity_mapper[row['source_secondary_entity']]
                    rule_df.loc[index, 'source_secondary_entity_ids'] = source_secondary_entity_id
            if not pd.isna(target_secondary_entity):
                if ',' in target_secondary_entity:
                    target_secondary_entity = row['target_secondary_entity'].split(',')
                    target_secondary_entity_ids = [entity_mapper[x.strip()] for x in target_secondary_entity]
                    target_secondary_entity_ids_string = ', '.join(target_secondary_entity_ids)
                    rule_df.loc[index, 'target_secondary_entity_ids'] = target_secondary_entity_ids_string
                else:
                    target_secondary_entity_id = entity_mapper[row['target_secondary_entity']]
                    rule_df.loc[index, 'target_secondary_entity_ids'] = target_secondary_entity_id
        return rule_df

    def build_rule_properties(self, rule_df):
        rule_props_dict = {}

        rule_props_columns = [x for x in rule_df.columns if x.startswith('property') or x.startswith('value') or x == 'rule_id']
        rule_props_df = rule_df[rule_props_columns]
        rule_props_df1 = pd.DataFrame(columns=['rule_prop_id', 'rule_id', 'prop_key', 'prop_value'])

        for index, row in rule_props_df.iterrows():
            for i in range(1,11):
                try:
                    if isNaN(row['property_'+str(i)]):
                        rule_props_df1.loc[len(rule_props_df1)] = [get_unique_id(), row['rule_id'],
                                                                    row['property_'+str(i)],
                                                                    row['value_'+str(i)]
                                                                    ]
                except KeyError:
                    break

        # rt_properties_df = rt_properties_df[(rt_properties_df.rule_template_prop_type  == 'PREDEFINED')]
        # rule_template_ids = set(rt_properties_df['rule_template_id'].to_list())
        # print(rule_template_ids)
        # for index, row in rule_df.iterrows():
        #     rt_properties_df1 = rt_properties_df[(rt_properties_df.rule_template_id  == row['rule_template_id'])]
        #     if rt_properties_df1.shape[0] == 0: continue
        #     for index1, row1 in rt_properties_df1.iterrows():
        #         if row['rule_template_id'] in rule_template_ids:
        #             rule_props_df1.loc[len(rule_props_df1)] = [get_unique_id(), row['rule_id'], row1['rule_template_prop_key'],
        #                                                         row1['rule_template_prop_type'], row1['rule_template_prop_value']
        #                                                         ]
                
        rule_props_df1 = add_who_columns(rule_props_df1)
        insert_into_db('rule_properties', rule_props_db_columns, rule_props_df1, rule_props_df_columns)
        rule_props_df1.to_csv('output/rule_properties.csv', index= False)

        
    def build(self, rule_df, entity_mapper, ruleset_mapper):
        new_ids = get_new_ids(rule_df)
        rule_df['rule_id'] = new_ids

        # rule_df = rule_df[[x for x in rule_df.columns if not(x.startswith('prop_'))]]
        # mapping entities and ruleset ids
        rule_df['ruleset_id'] = rule_df['ruleset_name'].map(ruleset_mapper)
        rule_df['source_entity_id'] = rule_df['source_entity'].map(entity_mapper)
        rule_df['target_entity_id'] = rule_df['target_entity'].map(entity_mapper)
        # mapping secondary entities with ids
        rule_df = self.map_secondary_entity(rule_df, entity_mapper)

        # mapping template ids to rule records
        rule_template_df = read_file('data_files/Bulk_upload_template_HSBC.xlsx', 'rule_template')
        map_templates = MapTemplates()
        rule_df = map_templates.map_rule_template(rule_template_df, rule_df)
        rule_df = rule_df.drop_duplicates()

        insert_into_db('rule', rule_db_columns, rule_df, rule_df_columns)

        # creating rule entity map table
        self.build_rule_entity_map(rule_df)

        # creating rule properties table - only variable properties
        self.build_rule_properties(rule_df)

        rule_df = rule_df[rule_columns]
        rule_df = add_who_columns(rule_df)
        rule_df.to_csv('output/rule.csv', index= False)

        return rule_df

