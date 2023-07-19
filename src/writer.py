import os


def big_query(data, context, entity_name):
    data.write \
        .format('bigquery') \
        .option('temporaryGcsBucket', context.get_value['temp_gcs_bucket_name']) \
        .mode("append") \
        .save(context.get_value['bq_dataset'] + '.' + entity_name)


def csv(data, context, is_local_run):
    data.write.options(header='True', delimiter=',') \
        .csv(context.get_value('output_path'))


def csv_local(data, context, entity_name):
    file_name = os.path.join(context.get_value('local_output_path'), entity_name + '.csv')
    data.toPandas().to_csv(file_name, index=False)


def parquet(data, context):
    data.write.parquet(context.get_value('output_path'))


def hive(data, context, entity_name):
    data.write \
        .mode("append") \
        .saveAsTable(context.get_value('hive_database') + '.' + entity_name)


# function to write to terminal
def console(data):
    data.show()


def write(data, context, entity_name):
    result_entity_type = context.get_value('result_entity_type')
    if result_entity_type == 'csv':
        csv(data, context)
    if result_entity_type == 'parquet':
        parquet(data, entity_name)
    if result_entity_type == 'big_query':
        big_query(data, entity_name, context)
    if result_entity_type == 'hive':
        return hive(data, entity_name, context)
    if result_entity_type == 'console':
        console(data)
    if result_entity_type == 'csv_local':
        csv_local(data, entity_name, context)
