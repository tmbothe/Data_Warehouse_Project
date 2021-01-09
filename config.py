import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open('clust.cfg'))

#1.1 Create the role, 
def create_role(role_name):
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
    
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=role_name,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=role_name)['Role']['Arn']

    return roleArn

#Create redshift cluster

def create_redshift_cluster(roleArn,cluster_type,node_type,num_nodes,db_user,db_password,cluster_identifier,db_name):
    try:
        response = redshift.create_cluster(        
        #HW
        ClusterType=cluster_type,
        NodeType=node_type,
        NumberOfNodes=int(num_nodes),

        #Identifiers & Credentials
        DBName=db_name,
        ClusterIdentifier=cluster_identifier,
        MasterUsername=db_user,
        MasterUserPassword=db_password,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
       )
        return response
    except Exception as e:
        print(e)
    

def delete_cluster(cluster_identifier):
    redshift.delete_cluster( ClusterIdentifier=cluster_identifier,  SkipFinalClusterSnapshot=True)

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])    




if  __name__=='__main__':

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    iam = boto3.client('iam',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

    redshift = boto3.client('redshift',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

    df = pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })

    print(df)

    #Create role
    roleArn = create_role(DWH_IAM_ROLE_NAME)

    #create redshift cluster
    create_redshift_cluster(roleArn,cluster_type=DWH_CLUSTER_TYPE,node_type=DWH_NODE_TYPE,num_nodes=DWH_NUM_NODES,db_user=DWH_DB_USER,db_password=DWH_DB_PASSWORD,cluster_identifier=DWH_CLUSTER_IDENTIFIER,db_name=DWH_DB)
    
    #delete cluster 
    #delete_cluster(DWH_CLUSTER_IDENTIFIER)
    #print('creating redshift clusrer....')
    #create_redshift_cluster('arn:aws:iam::629256008499:role/dwhRole')

    #print('Redshift creation completed....')
    #myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    #prettyRedshiftProps(myClusterProps)