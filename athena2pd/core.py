import boto3
import pandas
import time
import io

class AthenaDFConnector:
    '''
    The AthenaDFConnector connects to your AWS Athena and S3 services and allows you to execute a simple
    SQL format query on an Athena Database and get a pandas dataframe returned.

    Attributes
    --
    profile_name : str
         - the name of the profile found in .aws/config file.
    
    athena_client : Service client instance
         - a Service Client instance of Amazon Athena (from the boto3 Session.client method).

    s3_client : Service client instance
         - a Service Client instance of Amazon S3 (from the boto3 Session.client method).

    Methods
    --
    query(query_string = '', s3_output_location = '') : pandas.DataFrame
         - queries your Athena database, and generates a pandas DataFrame from the output file stored at the s3 output location.
    '''

    def __init__(self, aws_profile_name = ''):
        '''
        Parameters
        --
        aws_profile_name : str
             - the name of the profile found in .aws/config file. 

        Raises
        --
        NotImplementedError
             - raised if no AWS profile name provided.
        
        ConnectionError
             - raised if unable to find profile name, or unable to connect to both Athena and S3 client services.
        '''
        if aws_profile_name == '':
            raise NotImplementedError('No AWS profile name provided class')
        self.profile_name = aws_profile_name
        try:
            # Try to create the Athena and S3 client services
            dev = boto3.Session(profile_name=self.profile_name)
            self.athena_client = dev.client(service_name='athena')
            self.s3_client = dev.client(service_name='s3')
        except Exception as e:
            raise ConnectionError('''Could not connect to AWS Services with this profile name.
                Please check to make the credentials in the .aws/credentials file and is setup for both Athena and S3,
                and that the profile name in .aws/config matches the supplied profile name.
                \nError: {}'''.format(e))

    def query(self, query_string = '', s3_output_location = ''):
        '''
        Queries your Athena database, and generates a pandas DataFrame from the output file stored at the s3 output location.

        Parameters
        --
        query_string : str
             - The SQL like string that will be used to query your Athena database.

        s3_output_location: str
             - the path of the S3 Bucket subfolder where the output file will generate to.
                EXAMPLE: s3://{bucket_name}/sub/folder

        Returns
        --
        pandas.DataFrame
             - the DataFrame representation of the query provided.
        '''
        # Start the query, get the id back
        execution_id = self.__start_query(query_string, s3_output_location)
        # Loop until you get a successful query
        self.__check_if_query_complete(execution_id)
        # Return a dataframe of the generated S3 object
        return self.__convert_to_dataframe(execution_id, s3_output_location)
        


    def __start_query(self, query_string = '', s3_output_location = ''):
        '''
        Step 1/3: Send the query to Athena, and return the query execution id for future steps.

        Parameters
        --
        query_string : str
             - The SQL like string that will be used to query your Athena database.

        s3_output_location : str
             - the path of the S3 Bucket subfolder where the output file will generate to.
                EXAMPLE: s3://{bucket_name}/sub/folder

        Returns
        --
        int
             - the query's execution ID.
        '''
        response = self.athena_client.start_query_execution(
            QueryString=query_string,
            ResultConfiguration={
                'OutputLocation': s3_output_location
            }
        )
        return response['QueryExecutionId']

    def __check_if_query_complete(self, execution_id = 0):
        '''
        Step 2/3: Wait for the query to either succeed or fail.

        Parameters
        --
        execution_id : int
             - the query's execution ID.

        Raises
        --
        RuntimeError
             - raised if the query fails or is cancelled.
        '''
        waitingForCompletion = True
        while waitingForCompletion:
            # Check the query's progress
            result = self.athena_client.get_query_execution(QueryExecutionId=execution_id)
            result_state = result['QueryExecution']['Status']['State']
            if result_state == 'SUCCEEDED':
                waitingForCompletion = False
            elif result_state == 'FAILED' or result_state == 'CANCELLED':
                raise RuntimeError('Query failed or was cancelled by user. Please also check to make sure the syntax of the SQL string is correct.')
            else:
                # if the state is RUNNING or QUEUED
                time.sleep(2)

    def __convert_to_dataframe(self, execution_id = 0, s3_output_location = ''):
        '''
        Step 3/3: Once query is complete, get CSV from S3 bucket, and return the Dataframe of the CSV.

        Parameters
        --
        execution_id : int
             - the query's execution ID.

        s3_output_location : str
             - the path of the S3 Bucket subfolder where the output file will generate to.
                EXAMPLE: s3://{bucket_name}/sub/folder

        Returns
        --
        pandas.DataFrame
             - the DataFrame representation of the query provided.

        Raises
        --
        FileNotFoundError
             - raised if unable to get the generated CSV file from the S3 bucket after query completed.
        '''
        # Separate the bucket name from the rest of the subfolders
        split_s3_name = s3_output_location.replace('s3://', '').split('/', 1)
        bucket_name = split_s3_name[0]
        key = split_s3_name[1]

        # Get the file from S3
        try:
            file_obj = self.s3_client.get_object(Bucket=bucket_name, Key='{}/{}.csv'.format(key, execution_id))
        except Exception as e:
            raise FileNotFoundError('Could not find generated CSV file in S3.\nError: {}'.format(e))

        # Convert the file to CSV then to Pandas Dataframe
        return pandas.read_csv(io.BytesIO(file_obj['Body'].read()))
    
