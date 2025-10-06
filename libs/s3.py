import boto3
import json
import io

class S3:    
    @staticmethod
    def get_client(credentials=None):
        """
        Returns S3 client
        """
        if credentials:
            return boto3.client('s3', **credentials)
        return boto3.client('s3')
    
    @staticmethod
    def create_bucket(bucket_name, location='eu-west-1',credentials=None):
        """
        Creates bucket
        
        Params:
            bucket_name (str): name of bucket
            location (str): S3 region
        """
        client = S3.get_client(credentials)
        response = client.create_bucket(Bucket = bucket_name,
                                        CreateBucketConfiguration={'LocationConstraint': location})
        
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(response)
        
        print(f'Bucket "{bucket_name}" created!')
        
    @staticmethod
    def delete_bucket(bucket_name, credentials=None):
        """
        Delete empty (!!!) bucket
        
        Params:
            bucket_name (str): name of bucket
        """
        client = S3.get_client(credentials)
        response = client.delete_bucket(Bucket=bucket_name)
        
        if response['ResponseMetadata']['HTTPStatusCode'] != 204:
            raise Exception(response)
        
        print(f'Bucket "{bucket_name}" deleted!')
    
    @staticmethod
    def create_bucket_if_not_exists(bucket_name, credentials=None):
        """
        If bucket with bucket_name does not exists => creates new one
        """
        available_buckets = S3.get_buckets()
        if bucket_name not in available_buckets:
            S3.create_bucket(bucket_name)
        
    @staticmethod
    def get_buckets(with_creation_date=False, credentials=None):
        """
        Returns all available buckets names 
        
        Params:
            with_creation_date (bool): if True => return also creation date of buckets
            
        Returns:
            list with names or list with dictionaries containing buckets info
        """
        client = S3.get_client(credentials)
        response = client.list_buckets()
        
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(response)
        
        if with_creation_date:
            return response['Buckets']
        
        return [bucket['Name'] for bucket in response['Buckets']]
    
    @staticmethod
    def create_json_in_bucket_if_not_exists(bucket_name, file_name, initial_json=None, credentials=None):
        """
        Creates json in bucket if json not exists
        
        Params:
            bucket_name (str): name of bucket where to store file
            file_name (str): path to file 
            initial_json (None or dumped json): json to store
        """
        filenames = S3.get_all_objects_from_bucket(bucket_name = bucket_name, 
                                                   prefix = file_name, 
                                                   only_keys = True)
        if file_name not in filenames:
            if initial_json is None:
                initial_json = json.dumps({})
            
            S3.store_file_in_bucket(bucket_name = bucket_name,
                                    file_name = file_name,
                                    file = initial_json)
    
    @staticmethod
    def store_file_in_bucket(bucket_name, file_name, file, credentials=None):
        """
        Stores file in bucket
        
        Params:
            bucket_name (str): name of bucket where to store file
            file_name (str): path to file
            file (dumped json or binary): file to store
        """
        client = S3.get_client(credentials)
        response = client.put_object(Bucket = bucket_name,
                                     Key = file_name,
                                     Body = file)
        
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(response)
        
        print(f'"{file_name}" succcesfully stored in "{bucket_name}" bucket!')
    
    @staticmethod
    def get_file_from_bucket(bucket_name, file_name, as_json=False, credentials=None):
        """
        Ger file from bucket
        
        Params:
            bucket_name (str): name of bucket where to store file
            file_name (str): path to file
            as_json (boo): if True => convert response body to json
            
        Returns:
            json or bytes object
        """
        client = S3.get_client(credentials)
        response = client.get_object(Bucket = bucket_name,
                                     Key = file_name)
        
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(response)
        
        body = response['Body'].read()
        
        if as_json: 
            return json.loads(body) 
        
        return io.BytesIO(body)
        
    @staticmethod
    def get_all_objects_from_bucket(bucket_name, prefix='', only_keys=True, credentials=None):
        """
        Get all object from bucket
        
        Params:
            bucket_name (str): name of bucket where to store file
            prefix (str): file filter
            only_keys (bool): if True => returns only filenames
            
        Returns:
            list with filenames or list with dictionaries containing files info
        """
        client = S3.get_client(credentials)

        kwargs = {
            'Bucket': bucket_name,
            'Prefix': prefix,    
        }
        
        data = []
        while True:
            response = client.list_objects_v2(**kwargs)
            
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception(response)
            
            if only_keys:
                data += [c.get('Key') for c in response.get('Contents',[])]
            else:
                data += response.get('Contents',[])

            try:
                kwargs['ContinuationToken'] = response['NextContinuationToken']
            except KeyError:
                break

        return data