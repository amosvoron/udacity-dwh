import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError

class Cluster:
    """Class that exposes fuctions for Amazon Redshift cluster management."""    
    
   
    def __init__(self, access_key, secret_access_key):
        """Class constructor that sets access keys and configuration.
    
        Args:
            access_key: The AWS access key. 
            secret_access_key: The AWS secret access key.
        
        Returns:
            None
        """        
        self.key = access_key
        self.secret = secret_access_key
        self.set_config()

        
    @staticmethod
    def getConnString():
        """Static function that returns the connection string 
        to connect to the Amazon Redshift database.
    
        Returns:
            str: The connection string.
        """                
        config = configparser.ConfigParser()
        config.read_file(open('dwh.cfg'))
        user = config.get("DB","DB_USER")
        password = config.get("DB","DB_PASSWORD")
        port = config.get("DB","DB_PORT")
        db = config.get("DB","DB_NAME")
        endpoint = config.get("DB","HOST")
        return f'postgresql://{user}:{password}@{endpoint}:{port}/{db}'
  

    # set config
    def set_config(self):
        """Sets configuration data defined in dwh.cfg configuration file.
    
        Returns:
            None
        """                        
        config = configparser.ConfigParser()
        config.read_file(open('dwh.cfg'))

        self.DWH_CLUSTER_TYPE       = config.get("CLUSTER","CLUSTER_TYPE")
        self.DWH_NUM_NODES          = config.get("CLUSTER","CLUSTER_NUM_NODES")
        self.DWH_NODE_TYPE          = config.get("CLUSTER","CLUSTER_NODE_TYPE")
        self.DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
        self.DWH_DB                 = config.get("DB","DB_NAME")
        self.DWH_DB_USER            = config.get("DB","DB_USER")
        self.DWH_DB_PASSWORD        = config.get("DB","DB_PASSWORD")
        self.DWH_PORT               = config.get("DB","DB_PORT")
        self.DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "IAM_ROLE_NAME")
        self.CLUSTER_ENDPOINT       = config.get("CLUSTER", "IAM_ROLE_NAME")
        
        print('Cluster configuration set.')
        

    def show_config(self):
        """Shows the configuration data.
    
        Returns:
            config_df (pandas DataFrame): The dataframe with configuration data.
        """          
        # Return params in dataframe
        config_df = pd.DataFrame({
            "Param": [
                "DWH_CLUSTER_TYPE", 
                "DWH_NUM_NODES", 
                "DWH_NODE_TYPE", 
                "DWH_CLUSTER_IDENTIFIER", 
                "DWH_DB", 
                "DWH_DB_USER", 
                "DWH_DB_PASSWORD", 
                "DWH_PORT", 
                "DWH_IAM_ROLE_NAME"],
            "Value": [
                self.DWH_CLUSTER_TYPE, 
                self.DWH_NUM_NODES, 
                self.DWH_NODE_TYPE, 
                self.DWH_CLUSTER_IDENTIFIER, 
                self.DWH_DB, 
                self.DWH_DB_USER, 
                self.DWH_DB_PASSWORD, 
                self.DWH_PORT, 
                self.DWH_IAM_ROLE_NAME]
        })   
        
        print(config_df)
                
   
    def create_clients(self):
        """Creates various clients needed by the Redshift cluster.
    
        Returns:
            None
        """                  
        self.ec2 = boto3.resource('ec2',
                             region_name="us-west-2",
                             aws_access_key_id=self.key,
                             aws_secret_access_key=self.secret
                            )

        self.s3 = boto3.resource('s3',
                            region_name="us-west-2",
                            aws_access_key_id=self.key,
                            aws_secret_access_key=self.secret
                           )

        self.iam = boto3.client('iam',
                           region_name='us-west-2',                       
                           aws_access_key_id=self.key,
                           aws_secret_access_key=self.secret
                          )

        self.redshift = boto3.client('redshift',
                                     region_name='us-west-2',                                
                                     aws_access_key_id=self.key,
                                     aws_secret_access_key=self.secret
                                    )
    
    
    def create_iam_role(self):
        """Creates IAM role.
    
        Returns:
            None
        """                  
        try:
            print("Creating a new IAM Role...") 
            dwhRole = self.iam.create_role(
                Path='/',
                RoleName=self.DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {
                        'Statement': [
                            {
                                'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'redshift.amazonaws.com'
                                }
                            }
                        ],
                        'Version': '2012-10-17'
                    })
            )    
        except Exception as e:
            print(e)
        
        
    def set_iam_role_arn(self): 
        """Stores IAM role in the cluster object.
    
        Returns:
            None
        """                  
        self.arn = self.iam.get_role(RoleName=self.DWH_IAM_ROLE_NAME)['Role']['Arn']
        print(f'ARN: {self.arn}')
    
    
    def attach_role_policy(self):
        """Attached role policy.
    
        Returns:
            None
        """                  
        self.iam.attach_role_policy(
            RoleName=self.DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']
        print("Role policy attached.")
        
        
    def prepare(self):
        """Carries out the preparation steps before creating the cluster.
    
        Returns:
            None
        """                  
        self.create_clients()
        self.create_iam_role()
        self.set_iam_role_arn()
        self.attach_role_policy()        
        
        
    def create(self):  
        """Creates the cluster.
    
        Returns:
            None
        """                  
        self.prepare()
        
        try:
            response = self.redshift.create_cluster(        
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),
                DBName=self.DWH_DB,
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD,
                IamRoles=[self.arn]  
            )    
            print('Amazon Redshift cluster is being created. Please wait...')
        except Exception as e:
            print(e)        
         

    def delete(self):
        """Deletes the cluster.
    
        Returns:
            None
        """                  
        self.redshift.delete_cluster(
            ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER, 
            SkipFinalClusterSnapshot=True)  
        print('Amazon Redshift cluster is being deleted. Please wait...')
        
        
    def get_info(self):
        """Returns the cluster information."""                  
        return self.redshift.describe_clusters(
            ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]


    def get_status(self):
        """Returns the cluster status."""  
        info = self.get_info()
        return info['ClusterStatus']


    def get_endpoint(self):
        """Returns the cluster endpoint."""      
        info = self.get_info()
        return info['Endpoint']['Address']
 
        
    def open_tcp(self):
        """Open TCP port.""" 
        try:
            info = self.get_info()
            vpc = self.ec2.Vpc(id=info['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(self.DWH_PORT),
                ToPort=int(self.DWH_PORT)
            )
        except Exception as e:
            print(e)
            
            
    def get_conn_string(self):
        """Returns the connection string to the cluster's DWH database."""      
        endpoint = self.get_endpoint()
        return f'postgresql://{self.DWH_DB_USER}:{self.DWH_DB_PASSWORD}@{endpoint}:{self.DWH_PORT}/{self.DWH_DB}'


    def delete_iam_role(self):
        """Deletes the IAM role."""      
        self.iam.detach_role_policy(RoleName=self.DWH_IAM_ROLE_NAME, 
                                    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        self.iam.delete_role(RoleName=self.DWH_IAM_ROLE_NAME)
        print('IAM role deleted.')
    
