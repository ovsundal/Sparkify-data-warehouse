import configparser

import boto3


def create_cluster(config):
    """
    Creates a redshift cluster based on params in dwh.cfg
    :param config:
    :return:
    """
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
    DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
    DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')

    DWH_DB = config.get('DWH', 'DWH_DB')
    DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
    DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')

    redshift = boto3.client('redshift',
                            region_name="eu-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    response = redshift.create_cluster(
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
    print(response)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    create_cluster(config)


if __name__ == "__main__":
    main()
