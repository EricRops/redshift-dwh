import configparser
import psycopg2
import pandas as pd
import boto3
          
def create_dwh(KEY, SECRET, ARN, cluster_type, node_type, num_nodes, db_name, cluster_id, db_user, db_password):
    """ 
    1. Create a redshift client
    2. Use the client to create a redshift cluster on AWS
    
    Input Credentials:
    - KEY: AWS key to your account, NEVER SHARE THIS unless you want your account stolen
    - SECRET: AWS secret to your account, NEVER SHARE THIS unless you want your account stolen
    - ARN: IAM user ARN with S3 Read Access
    DWH params:
    - cluster_type: CPU specs for AWS cluster (we use dc2.large) 
    - node_type: multi-node or single node?
    - num_nodes: how many nodes to use? (use 1 for testing)
    - db_name: database name
    - cluster_id: simple cluster identifier (text)
    - user: username who accesses the cluster
    - password: password to access the cluster
    """
    # Create redshift client      
    redshift = boto3.client('redshift',
       region_name="us-west-2",
       aws_access_key_id=KEY,
       aws_secret_access_key=SECRET)
          
    # Create cluster
    # If multi-node, keep the num_nodes argument
    if node_type == "multi-node":
        response = redshift.create_cluster(
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_nodes),
            # Identifiers & Credentials
            DBName=db_name,
            ClusterIdentifier=cluster_id,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            #Roles (for s3 access)
            IamRoles=[ARN])
    # If single-node, remove the num_nodes argument
    else:
        response = redshift.create_cluster(
            ClusterType=cluster_type,
            NodeType=node_type,
            # Identifiers & Credentials
            DBName=db_name,
            ClusterIdentifier=cluster_id,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            # Roles (for s3 access)
            IamRoles=[ARN])

def main():
    """
    - Pull DB parameters from the configuration file
    - Create redshift cluster on AWS
    """
    
    # Pull DB params and credentials
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DB_CLUSTER_TYPE       = config.get("SPECS","DB_CLUSTER_TYPE")
    DB_NODE_TYPE          = config.get("SPECS","DB_NODE_TYPE")
    DB_NUM_NODES          = config.get("SPECS","DB_NUM_NODES")
    DB_CLUSTER_ID         = config.get("SPECS","DB_CLUSTER_ID")    
    DB_NAME                 = config.get("CLUSTER","DB_NAME")
    DB_USER                 = config.get("CLUSTER","DB_USER")
    DB_PASSWORD             = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT                 = config.get("CLUSTER","DB_PORT")
    ARN                     = config.get("IAM_ROLE", "ARN")

    # Display the DB specs (NEVER SHOW THE KEY AND SECRET)
    df = pd.DataFrame({"Param":
                      ["DB_CLUSTER_TYPE", "DB_NUM_NODES", "DB_NODE_TYPE", "DB_CLUSTER_ID", \
                       "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT"],
                      "Value":
                      [DB_CLUSTER_TYPE, DB_NUM_NODES, DB_NODE_TYPE, DB_CLUSTER_ID, \
                       DB_NAME, DB_USER, DB_PASSWORD, DB_PORT]
                 })
    print("DB Parameters:")
    print(df)
    
    # Create cluster
    try:
        create_dwh(KEY, SECRET, ARN, DB_CLUSTER_TYPE, DB_NODE_TYPE, DB_NUM_NODES, DB_NAME, DB_CLUSTER_ID, DB_USER, DB_PASSWORD)        
        print("Creating Cluster..............................")        
    except Exception as e:
        print(e)
          
if __name__ == "__main__":
    main()