import configparser
import json
import boto3


def create_clients(KEY, SECRET, REGION):
    iam = boto3.client('iam', aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name=REGION)

    ec2 = boto3.resource('ec2',
                         region_name=REGION,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )

    return iam, ec2


def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    Creates an iam role (add administrator access from inside AWS), adds policy and returns the ARN
    """
    try:
        print('creating IAM role...')
        iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )

        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                               )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print(e)


def create_cluster(REGION, KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB,
                   DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn):
    """
    Creates a redshift cluster based on params in dwh.cfg
    """

    redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    print('creating cluster...')
    try:
        redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[roleArn]
        )
    except :
        print('Error, could not create cluster')


def open_cluster_endpoint(ec2, myClusterProps, DWH_PORT):
    """
    Open an incoming TCP port to access the cluster endpoint
    """
    try:
        print('Open incoming TCP port to access cluster endpoint...')
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # get params from env
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
    DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
    DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')

    DWH_DB = config.get('DWH', 'DWH_DB')
    DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    DWH_DB_USER = config.get('CLUSTER', 'DB_USER')
    DWH_DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    REGION = config.get('CLUSTER', 'REGION')
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")


    # get clients
    iam, ec2 = create_clients(KEY, SECRET, REGION)

    # create iam role and get roleArn
    create_iam_role(iam, DWH_IAM_ROLE_NAME)
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    # create cluster
    create_cluster(REGION, KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB,
                   DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn)

    # ONLY RUN THIS ONCE THE CLUSTER IS AVAILABLE
    # open an incoming TCP port to access the cluster endpoint

    # DWH_PORT = config.get("DWH", "DWH_PORT")
    # redshift = boto3.client('redshift',
    #                         region_name=REGION,
    #                         aws_access_key_id=KEY,
    #                         aws_secret_access_key=SECRET)
    #
    # myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    # open_cluster_endpoint(ec2, myClusterProps, DWH_PORT)


if __name__ == "__main__":
    main()
