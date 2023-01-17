from pyspark.sql import SparkSession
import argparse
from functions import rename_data, filter_data, select_data
import logging
import os


def get_arguments() -> tuple[list[str], str, str]:
    """ Function gets arguments provided by user. It looks for clients raw data path, financial raw data path
     and list of countries to filter data to

    Returns
    -------

    """
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
    """ Main function. Here function gets arguments provided by user. Gets 2 dataframes from provided csv path,
    renamed columns in financial_df, joins both dataframes, filters dataframe based on list of countries provided by
    user and whether account is active. Finally selects only necessary columns and saves result dataframe to csv
    in client_data directorycd
    """
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
    logging.info('Joining dataframes')
    joined_df = clients_df.join(financial_df_renamed, ['id'])
    selected_columns = ['id', 'email', 'credit_card_type', 'account_type']
    columns_to_filter = {'country': list_countries, 'active': [True]}
    logging.info('Filtering result dataframe')
    filtered_df = filter_data(input_df=joined_df, columns_to_filter=columns_to_filter)
    selected_df = select_data(filtered_df, selected_columns)
    results_path = os.path.join(os.getcwd(), 'client_data')
    logging.info(f'Saving results to csv file: {results_path}')
    selected_df.coalesce(1).write.option("header", "true").format("csv").mode('overwrite').save(results_path)


if __name__ == "__main__":
    logging.basicConfig(filename='myapp.log', level=logging.INFO)
    logging.info('Started')
    main()
    logging.info('Finished')




