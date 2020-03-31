import boto3
import pandas
import time
import io

class AthenaDFConnector:

    profile_name = None
    athena_client = None
    s3_client = None

    def __init__(self, aws_profile_name):
        self.profile_name = aws_profile_name
        try:
            dev = boto3.Session(profile_name=self.profile_name)
            self.athena_client = dev.client(service_name='athena')
            self.s3_client = dev.client(service_name='s3')
        except Exception as e:
            raise ConnectionError('Could not connect to AWS Services with this profile name. Please check to make sure the profile is in the .aws/credentials file and is setup for both Athena and S3.\nError: {}'.format(e))

    def query(self, query_string, s3_output_location):
        # Start the query, get the id back
        execution_id = self.__start_query(query_string, s3_output_location)
        # Loop until you get a successful query
        self.__check_if_query_complete(execution_id)
        # Return a dataframe of the generated S3 object
        return self.__convert_to_dataframe(execution_id, s3_output_location)
        


    def __start_query(self, query_string, s3_output_location):
        response = self.athena_client.start_query_execution(
            QueryString=query_string,
            ResultConfiguration={
                'OutputLocation': s3_output_location
            }
        )
        return response['QueryExecutionId']

    def __check_if_query_complete(self, execution_id):
        waitingForCompletion = True
        while waitingForCompletion:
            result = self.athena_client.get_query_execution(QueryExecutionId=execution_id)
            if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                waitingForCompletion = False
            else:
                time.sleep(2)

    def __convert_to_dataframe(self, execution_id, s3_output_location):
        # Separate the bucket name from the rest of the subfolders
        split_s3_name = s3_output_location.replace('s3://', '').split('/', 1)
        bucket_name = split_s3_name[0]
        key = split_s3_name[1]

        file_obj = self.s3_client.get_object(Bucket=bucket_name, Key='{}/{}.csv'.format(key, execution_id))
        return pandas.read_csv(io.BytesIO(file_obj['Body'].read()))
    
