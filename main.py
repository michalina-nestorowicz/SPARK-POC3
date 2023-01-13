from pyspark.sql import SparkSession
import argparse
from functions import rename_data, filter_data, join_data, select_data
import logging
import os

def get_arguments() -> tuple[list[str], str, str]:

    parser = argparse.ArgumentParser()
    parser.add_argument('--clients', type=str, required=True)
    parser.add_argument('--financial', type=str, required=True)
    parser.add_argument(
        "--list",  
        nargs="*",  # 0 or more values expected => creates a list
        type=str,
        default=['Poland']  # default if nothing is provided
    )
    args = parser.parse_args()
    return args.list, args.clients, args.financial
    

def main():
    logging.info('Getting arguments')
    list_countries, path_clients, path_financial = get_arguments()
    spark = SparkSession.builder.appName("POC3").getOrCreate()
    logging.info('Reading Dataframe from csv')
    clients_df = (spark
           .read
           .option("sep", ",")
           .option("header", True)
           .option("inferSchema", True)
           .csv(path_clients)
          )

    financial_df = (spark
           .read
           .option("sep", ",")
           .option("header", True)
           .option("inferSchema", True)
           .csv(path_financial)
          )

    logging.info('Renaming column in financial df')
    column_to_rename_dict = {'cc_t': 'credit_card_type', 'cc_n': 'credit_card_number',
                             'a': 'active', 'cc_mc': 'credit_card_main_currency',
                             'ac_t': 'account_type'}
    financial_df_renamed = rename_data(financial_df, column_to_rename_dict)
    financial_df_renamed.show()
    logging.info('Filtering result dataframe')
    #joined_df = join_data(input1_df=clients_df, input2_df=financial_df_renamed, column_to_join_on='id')
    joined_df = clients_df.join(financial_df_renamed, ['id'])
    joined_df.show()
    selected_columns = ['id', 'email', 'credit_card_type', 'account_type']
    columns_to_filter = {'country': list_countries, 'active': [True]}
    filtered_df = filter_data(input_df=joined_df, columns_to_filter=columns_to_filter)
    filtered_df.show()
    select_df = select_data(filtered_df, selected_columns)
    select_df.show()
    results_path = os.path.join(os.getcwd(), 'client_data\\result')
    logging.info('Saving results to csv file')
    #filtered_df.toPandas().to_csv(results_path, index=True)
    #filtered_df.repartition(2).write.csv(results_path)
    filtered_df.repartition(1).write.option("header", "true").format("csv").save("C:\\GIT\\PYSPARK_UPSKILLING\\SPARK-POC3\\client_data\\results_folder")


if __name__ == "__main__":
    logging.basicConfig(filename='myapp.log', level=logging.INFO)
    logging.info('Started')
    main()
    logging.info('Finished')




