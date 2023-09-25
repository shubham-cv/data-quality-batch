import pandas as pd
from resources.util import *
from resources.constants import *
from pprint import pprint
import json


def create_dfs():
    for table in table_list:
        query= f"select * from {table};"
        exec(f"%s = pd.read_sql('{query}', conn)" % (table), globals())

def get_rule(rule, rule_entity_map, entity_properties, entity_template_properties, rule_properties, rule_template,rule_template_properties):
    rules = []
    rule1 = {}
    i = 0
    for index, row in rule.iterrows():
        i = i+1
        rule1['sequence'] = i
        rule1['status'] = 'active'
        rule_details = {}
        rule_details['id'] = row['rule_id']
        rule_details['name'] = row['rule_name']
        rule_details['description'] = row['rule_desc']
        rule_details['dq_metric'] = rule_template[rule_template['rule_template_id'] == row['rule_template_id']]['dq_metric'].values[0]
        rule_details['data_entity_associations'] = get_entity(row, rule_entity_map, entity_properties, entity_template_properties)
        rule_details['properties'] = get_rule_props(row, rule_properties, rule_template_properties)
        rule_details['template'] = get_rule_temp(row, rule_template, rule_template_properties)
        rule1['rule_details'] = rule_details.copy()
        rules.append(rule1.copy())

    return rules

def get_entity(rule_record, rule_entity_map, entity_properties, entity_template_properties):
    dea_list = []
    dea_dict = {}
    rule_entity_map = rule_entity_map[rule_entity_map['rule_id'] == rule_record['rule_id']]
    # print(rule_entity_map)
    for index1, row in rule_entity_map.iterrows():
        dea_dict['entity_id'] = row['entity_id']
        dea_dict['entity_name'] = entity[entity['entity_id'] == row['entity_id']]['entity_name'].values[0]
        dea_dict['entity_physical_name'] = entity[entity['entity_id'] == row['entity_id']]['entity_physical_name'].values[0]
        dea_dict['entity_behaviour'] = row['entity_behaviour']
        dea_dict['entity_type'] = 'FILE'
        dea_dict['entity_sub_type'] = 'CSV'
        dea_dict['primary_key'] = entity[entity['entity_id'] == row['entity_id']]['entity_primary_key'].values[0]
        dea_dict['is_primary'] = row['is_primary']
        dea_dict['properties'] = get_entity_props(row, entity_properties)
        dea_dict['all_entity_properties'] = get_entity_temp_props(entity[entity['entity_id'] == row['entity_id']]['entity_template_id'].values[0], entity_template_properties)
        dea_list.append(dea_dict.copy())
    
    return dea_list

def get_entity_props(entity_record, entity_properties):
    entity_properties = entity_properties[entity_properties['entity_id'] == entity_record['entity_id']]
    ep_list = []
    ep_dict = {}
    for index, row in entity_properties.iterrows():
        ep_dict['key'] = row['entity_prop_key']
        ep_dict['value'] = row['entity_prop_value']
        ep_list.append(ep_dict.copy())
    
    return ep_list

def get_entity_temp_props(entity_template_id, entity_template_properties):
    entity_template_properties = entity_template_properties[entity_template_properties['entity_template_id'] == entity_template_id]
    ep_list = []
    ep_dict = {}
    for index, row in entity_template_properties.iterrows():
        ep_dict['mandatory'] = row['is_mandatory']
        ep_dict['description'] = row['entity_template_prop_desc']
        ep_dict['key'] = row['entity_template_prop_key']
        ep_list.append(ep_dict.copy())
    
    return ep_list

def get_rule_props(rule_record, rule_properties, rule_template_properties):
    rule_properties = rule_properties[rule_properties['rule_id'] == rule_record['rule_id']]
    rp_list = []
    rp_dict = {}
    for index, row in rule_properties.iterrows():
        rp_dict['key'] = row['rule_prop_key']
        rp_dict['type'] = 'VARIABLE'
        rp_dict['value'] = row['rule_prop_value']
        rp_list.append(rp_dict.copy())
        rp_list.extend(get_rule_temp_props(rule_record, rule_template_properties))
    return rp_list

def get_rule_temp_props(rule_record, rule_template_properties):
    rule_template_properties = rule_template_properties[(rule_template_properties['rule_template_id'] == rule_record['rule_template_id']) \
                                                    & (rule_template_properties['rule_template_id'] == 'PREDEFINED')]
    rp_list = []
    rp_dict = {}
    for index, row in rule_template_properties.iterrows():
        rp_dict['key'] = row['rule_template_prop_key']
        rp_dict['type'] = row['rule_template_prop_type']
        rp_dict['value'] = row['rule_template_prop_value']
        rp_list.append(rp_dict.copy())
    return rp_list

def get_rule_temp(rule_record, rule_template, rule_template_properties):
    rule_template = rule_template[(rule_template['rule_template_id'] == rule_record['rule_template_id'])]
    rule_template_properties = rule_template_properties[(rule_template_properties['rule_template_id'] == rule_record['rule_template_id'])]

    rt_dict = {}
    for index, row in rule_template.iterrows():
        rt_dict['id'] = row['rule_template_id']
        rt_dict['name'] = row['rule_template_name']
        rt_dict['description'] = row['rule_template_desc']
        rtp_list = []
        rtp_dict = {}
        for index1, row1 in rule_template_properties.iterrows():
            rtp_dict['mandatory'] = row1['is_mandatory']
            rtp_dict['description'] = row1['rule_template_prop_desc']
            rtp_dict['type'] = row1['rule_template_prop_type']
            rtp_dict['key'] = row1['rule_template_prop_key']
            rtp_dict['value'] = ''
            if row1['rule_template_prop_value']:
                rtp_dict['value'] = row1['rule_template_prop_value']
            else:
                del rtp_dict['value']
            rtp_list.append(rtp_dict.copy())
        rt_dict['properties'] = rtp_list

    return rt_dict


    

conn = jdbc_connection()

create_dfs()

rule_json = {}
print("Create a rule json file for one rule or multiple rules or a ruleset")
# choice = input("Options: rule/rules/ruleset: ")
choice = 'ruleset'

if choice == 'ruleset':
    # rule_name = input("Enter rule_name: ")
    ruleset_name = 'dq.bank.account.payments'
    ruleset = ruleset[ruleset['ruleset_name'] == ruleset_name]

    for index, row in ruleset.iterrows():
        rule_json['ruleset_id'] = row['ruleset_id']
        rule_json['ruleset_name'] = row['ruleset_name']
        rule_json['ruleset_desc'] = row['ruleset_desc']
        rule_json['notification_email'] = row['notification_email']
    
    ruleset_id = ruleset[ruleset['ruleset_name'] == ruleset_name]['ruleset_id'].values[0]

    rule = rule[rule['ruleset_id'] == ruleset_id]

    rules = get_rule(rule, rule_entity_map, entity_properties, entity_template_properties, rule_properties, rule_template, rule_template_properties)  
    rule_json['rules'] = rules
    # pprint(rule_json)

    with open('rule_template.json', 'w', encoding='utf-8') as f:
        json.dump(rule_json, f, ensure_ascii=False, indent=4)




