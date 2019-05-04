import configparser
import json
import boto3


def create_clients(KEY, SECRET, REGION):
    iam = boto3.resource('iam', aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name=REGION)

    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )

    return iam, ec2


def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    Creates an iam role (add administrator access from inside AWS)
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
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        print(roleArn)
    except Exception as e:
        print(e)


def open_cluster_endpoint(ec2, myClusterProps, DWH_PORT):
    """
    Open an incoming TCP port to access the cluster endpoint
    """
    try:
        print('Open incoming TCP port to access cluster endpoint...')
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        print(vpc)

        defaultSg = list(vpc.security_groups.all())
        print(defaultSg)

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

def create_cluster(REGION, KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB,
                   DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
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
            MasterUserPassword=DWH_DB_PASSWORD
        )
        return redshift
    except :
        print('Error, could not create cluster')


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
    DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
    DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
    REGION = config.get('CLUSTER', 'REGION')
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_PORT = config.get("DWH", "DWH_PORT")

    # get clients
    iam, ec2 = create_clients(KEY, SECRET, REGION)

    # create iam role
    create_iam_role(iam, DWH_IAM_ROLE_NAME)

    # create cluster
    # create_cluster(REGION, KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB,
    #                DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)
    
    # describe cluster
    redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    open_cluster_endpoint(ec2, myClusterProps, DWH_PORT)




    # TODO: create iam role, setup policy etc
    # https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/445568fc-578d-4d3e-ab9c-2d186728ab22/lessons/21d59f40-6033-40b5-81a2-4a3211d9f46e/concepts/2c454710-4a84-4c4f-9aeb-caa76f816466#
    # https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/445568fc-578d-4d3e-ab9c-2d186728ab22/lessons/21d59f40-6033-40b5-81a2-4a3211d9f46e/concepts/7a66583b-36de-4516-8cf8-2962e7b45c43


if __name__ == "__main__":
    main()
