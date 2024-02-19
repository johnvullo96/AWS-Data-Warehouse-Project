import pandas as pd
import boto3
import json
import configparser
import time
import sys

config = configparser.ConfigParser()
config.read_file(open('cluster_setup.cfg'))

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


def create_resources():    
    ec2 = boto3.resource('ec2',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                       )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )

    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                       )
    print("Resources & Clients Created")
    return ec2, s3, iam, redshift

def create_iam_role(iam):
    #Creating New Role 
    try:
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
            )    
    except Exception as e:
        print(e)

    #Attaching policy to role
    try:
        print("Attaching Policy to Role")
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']
        print("Getting the IAM role ARN")
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    except Exception as e:
        print(e)
    
    return roleArn

def create_redshift_cluster(redshift, roleArn):
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)
        
def open_tct_port(ec2,redshift):
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_status = cluster_props['ClusterStatus']
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
        
 
def write_cluster_details_to_config(redshift):
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = cluster_props['ClusterStatus']
    DWH_ENDPOINT = cluster_props['Endpoint']['Address']
    DWH_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    config = configparser.ConfigParser()
    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

    config.set("CLUSTER", "HOST", cluster_props['Endpoint']['Address'])
    config.set("IAM_ROLE", "ARN", cluster_props['IamRoles'][0]['IamRoleArn'])

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)
        
    print("Wrote Cluster Details to Config File")
    
def delete_redshift_cluster(redshift):
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    print("Redshift Cluster Deleting")  

def delete_iam_role(iam):
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    print("IAM role deleted and policy detached")
    
def clear_cluster_details_from_config():
    config = configparser.ConfigParser()
    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

    config.set("CLUSTER", "HOST", "None")
    config.set("IAM_ROLE", "ARN", "None")

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)
    print("Cluster details cleared from config file")
    
def main(action):
    ec2, s3, iam, redshift = create_resources()
    
    if action == 'create':
        print("Cluster Creation Workflow Initiated")
        
        roleArn = create_iam_role(iam)
        
        create_redshift_cluster(redshift, roleArn)
        
        while True:
            cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            cluster_status = cluster_props['ClusterStatus']
            if cluster_status == 'creating':
                print("Cluster still being created")
                time.sleep(30)
                continue
            elif cluster_status == 'available':
                print("Cluster Available")
                break
                
        open_tct_port(ec2,redshift)
        
        write_cluster_details_to_config(redshift)
        
    elif action == 'delete':
        print("Cluster Deletion Workflow Initiated")
        
        delete_redshift_cluster(redshift)
        
        while True:
            try:
                cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
                cluster_status = cluster_props['ClusterStatus']
                if cluster_status == 'deleting':
                    print("Cluster still deleting")
                    time.sleep(30)
                    continue
            except Exception as e:
                print(e)
                break
                
        delete_iam_role(iam)
        
        clear_cluster_details_from_config()
        
    else:
        print("Action to delete or create not passed")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py [action]")
        sys.exit(1)
    
    action = sys.argv[1]
    main(action)


