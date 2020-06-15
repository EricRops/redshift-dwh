import configparser
import psycopg2
import pandas as pd
import boto3
          
def prettyRedshiftProps(props):
    """ Return a nice DF of select Redshift Cluster Properties"""
    
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", \
                  "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def main():
    """
    - Pull DB credentials from the configuration file
    - Retreive the redshift client
    - Return nice Pandas DF of the cluster properties
    """
    # Pull relevant credentials
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    KEY           = config.get('AWS','KEY')
    SECRET        = config.get('AWS','SECRET')
    DB_CLUSTER_ID = config.get("SPECS","DB_CLUSTER_ID") 
    
    # Retreive the redshift client      
    redshift = boto3.client('redshift',
       region_name="us-west-2",
       aws_access_key_id=KEY,
       aws_secret_access_key=SECRET)
        
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_ID)['Clusters'][0]
    df = prettyRedshiftProps(myClusterProps)
    print("DB Status:")
    print(df)
    
if __name__ == "__main__":
    main()