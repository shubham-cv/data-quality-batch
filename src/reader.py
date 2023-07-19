from src.utils import get_spark_session


def parquet(entity, query):
    path = [entity_property for entity_property in entity['properties']
                if entity_property['key'] == 'PATH'][0]['value']
    data = get_spark_session().read.parquet(path, header=True)
    data.registerTempTable(entity['entity_physical_name'])
    return get_spark_session().sql(query)


def csv(entity, query):
    path = [entity_property for entity_property in entity['properties']
            if entity_property['key'] == 'PATH'][0]['value']
    data = get_spark_session().read.csv(path, header=True)
    data.registerTempTable(entity['entity_physical_name'])
    return get_spark_session().sql(query)


def big_query(entity, query, context):
    get_spark_session().conf.set('temporaryGcsBucket', context.get_value('temporaryGcsBucket'))
    get_spark_session().conf.set('materializationDataset', context.get_value('materializationDataset'))

    data = get_spark_session().read.format('bigquery'). \
        option('project', context.get_value('gcp_bq_project')). \
        option('table', entity['entity_physical_name']). \
        load()

    data.registerTempTable(entity['entity_physical_name'])
    return get_spark_session().sql(query)


def hive(query):
    return get_spark_session().sql(query)


def read(entity, query, context):
    entity_sub_type = entity['entity_sub_type']
    data = None
    if entity_sub_type == 'csv':
        data = csv(entity, query, context)
    if entity_sub_type == 'big_query':
        data = big_query(entity, query, context)
    if entity_sub_type == 'hive':
        data = hive(query)
    return data
