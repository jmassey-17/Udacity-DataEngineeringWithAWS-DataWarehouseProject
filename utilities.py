import boto3
import json
from botocore.exceptions import ClientError

def create_iam_role(config): 
    """
    creates iam role on aws with information stored in config file

    Parameters
    ----------
    config : config
        config file for aws processes.

    Returns
    -------
    None.

    """
    # Open IAM client
    iam = boto3.client('iam',
                       aws_access_key_id=config.get('AWS', 'KEY'),
                       aws_secret_access_key=config.get('AWS', 'SECRET'),
                       region_name='us-west-2'
                      )
    
    # Check to see if role already exists
    try: 
        result = iam.get_role(RoleName=config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME"))
        config.set('IAM_ROLE', 'ARN', result['Role']['Arn'])
        
        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)
        
        if result:
            print("Role already exists. Using existing role.")
            return result['Role']['Arn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print("Creating a new IAM Role")
        else:
            raise
    
    # Create the IAM role
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME"),
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [
                        {
                            'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}
                        }
                    ],
                    'Version': '2012-10-17'
                }
            )
        )
        
        print("Attaching relevant policies")
        iam.attach_role_policy(
            RoleName=config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME"),
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']
        
        role_arn = iam.get_role(RoleName=config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME"))['Role']['Arn']
        config.set('IAM_ROLE', 'ARN', role_arn)
        
        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)
        
        return role_arn
    except Exception as e:
        print(f"Error creating IAM role: {e}")
        raise


import time

def create_redshift_cluster(config): 
    """
    creates redshift cluster on aws with information stored in config file

    Parameters
    ----------
    config : config
        config file for aws processes.

    Returns
    -------
    None.

    """
    """Checks for existing redshift client and creates one if none present"""
    # Initiate client 
    print("Initiating Redshift client")
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=config.get('AWS', 'KEY'),
                            aws_secret_access_key=config.get('AWS', 'SECRET')
                           )
    
    # Check for existing clusters
    try: 
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
        if myClusterProps['ClusterStatus'] in ['available', 'creating']:
            print("Cluster already available or in the process of being created.")
            config.set('CLUSTER', 'Host', myClusterProps['Endpoint']['Address'])
            with open('dwh.cfg', 'w') as configfile:
                config.write(configfile)
            
            return myClusterProps
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterNotFound':
            print("No cluster exists, making a new one")
        else:
            raise
    
    # Create the cluster
    try:
        response = redshift.create_cluster(        
            ClusterType=config.get("CLUSTER", "DWH_CLUSTER_TYPE"),
            NodeType=config.get("CLUSTER", "DWH_NODE_TYPE"),
            NumberOfNodes=int(config.get("CLUSTER", "DWH_NUM_NODES")),
            DBName=config.get("CLUSTER", "DWH_DB"),
            ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"),
            MasterUsername=config.get("CLUSTER", "DWH_DB_USER"),
            MasterUserPassword=config.get("CLUSTER", "DWH_DB_PASSWORD"),
            IamRoles=[config['IAM_ROLE']['ARN']]  
        )
        
        # Wait for cluster to be available
        while True:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
            if myClusterProps['ClusterStatus'] == 'available':
                break
            print("Waiting for cluster to become available...")
            time.sleep(30)  # Wait 30 seconds before checking again
        
        print("Cluster is available, updating config file")
        config.set('CLUSTER', 'Host', myClusterProps['Endpoint']['Address'])
        
        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)
        
        return myClusterProps
    except Exception as e:
        print(f"Error creating Redshift cluster: {e}")
        raise

    

def delete_redshift_cluster(config): 
    """
    removes redshift client

    Parameters
    ----------
    config : config
        config file for aws processes.

    Returns
    -------
    None.

    """
    """removes redshift client"""
    # Initiate client 
    print("Initiating Redshift client")
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=config.get('AWS', 'KEY'),
                            aws_secret_access_key=config.get('AWS', 'SECRET')
                           )
    
    # Delete cluster
    try:
        redshift.delete_cluster(
            ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"),
            SkipFinalClusterSnapshot=True
        )
        
        # Wait for deletion to complete
        while True:
            try:
                myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
                print("Waiting for cluster deletion...")
                time.sleep(30)  # Wait 30 seconds before checking again
            except ClientError as e:
                if e.response['Error']['Code'] == 'ClusterNotFound':
                    print(f"Cluster {config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')} deleted")
                    break
                else:
                    raise
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterNotFound':
            print("Cluster does not exist.")
        else:
            print(f"Error deleting cluster: {e}")
            raise
