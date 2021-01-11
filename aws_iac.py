import configparser
import json
import sys
import boto3
import botocore.exceptions
from collections import defaultdict

class AwsIac(object):
    def __init__(self, config):
        self.access_key = config['AWS_CONFIG']['ACCESS_KEY']
        self.secret_key = config['AWS_CONFIG']['SECRET_KEY']
        self.region = config['AWS_CONFIG']['REGION']
        self.iam_role_name= config['AWS_CONFIG']['ROLE_NAME']
        self.rs_cluster_identifier = config['AWS_CONFIG']['RS_CLUSTER_IDENTIFIER']
        self.rs_node_type = config['AWS_CONFIG']['RS_NODE_TYPE']
        self.rs_num_nodes = config['AWS_CONFIG']['RS_NUM_NODES']
        self.rs_cluster_type = config['AWS_CONFIG']['RS_CLUSTER_TYPE']
        self.rs_db_name = config['CLUSTER']['DB_NAME']
        self.rs_db_user = config['CLUSTER']['DB_USER']
        self.rs_db_password = config['CLUSTER']['DB_PASSWORD']
        self.rs_db_port = config['CLUSTER']['DB_PORT']
        self.arn_value = ''

    def update_config_file(self, param_dict):
        config = configparser.ConfigParser()
        config.read('dwh.cfg')
        config[param_dict['config_identifier']][param_dict['param_key']] = param_dict['param_value']
        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)
        self.arn_value= config['IAM_ROLE']['arn']

    def create_redshift_cluster(self):
        '''
            creates a red shift cluster and prints the cluster config once the cluster is available  

        '''
        self.create_redshift_iam_role()
        redshift_client = boto3.client('redshift', 
                            aws_access_key_id=self.access_key,
                            aws_secret_access_key= self.secret_key, 
                            region_name=self.region)
        ec2_client = boto3.resource('ec2',
                        aws_access_key_id=self.access_key,
                        aws_secret_access_key= self.secret_key, 
                        region_name=self.region)
        redshift_client.create_cluster(
                        ClusterIdentifier=self.rs_cluster_identifier,
                        ClusterType=self.rs_cluster_type,
                        NodeType=self.rs_node_type,
                        NumberOfNodes=int(self.rs_num_nodes),
                        DBName=self.rs_db_name,
                        MasterUsername=self.rs_db_user,
                        MasterUserPassword=self.rs_db_password,
                        IamRoles=[self.arn_value]
                    )
        waiter = redshift_client.get_waiter('cluster_available')
        try:
            waiter.wait(ClusterIdentifier=self.rs_cluster_identifier,
                            WaiterConfig={
                            'Delay': 60,
                            'MaxAttempts': 5
                            })
            print("Out of wait --->>>")
            response = redshift_client.describe_clusters(ClusterIdentifier=self.rs_cluster_identifier)
            cluster_dict = response['Clusters'][0]
            endpoint_address = ''
            cluster_status =''
            vpcID = ''
            for cluster_key, cluster_val in cluster_dict.items():
                if cluster_key == 'ClusterStatus':
                    cluster_status = cluster_val
                elif cluster_key == 'VpcId':
                    vpcID = cluster_val
                elif cluster_key == 'Endpoint':
                    for key, val in cluster_val.items():
                        if key == 'Address':
                            endpoint_address = val

            print('Redshift Endpoint Available Status {}'.format(cluster_status))
            print('Redshift Endpoint Address {}'.format(endpoint_address))
            print('Redshift vpcID {}'.format(vpcID))
            param_dict = defaultdict()
            param_dict['config_identifier'] = 'CLUSTER'
            param_dict['param_key'] = 'HOST'
            param_dict['param_value'] = endpoint_address
            self.update_config_file(param_dict)
            try:
                vpc = ec2_client.Vpc(id=vpcID)
                defaultSg = list(vpc.security_groups.all())[0]
                defaultSg.authorize_ingress(
                    GroupName= defaultSg.group_name,  
                    CidrIp='0.0.0.0/0',  
                    IpProtocol='TCP',  
                    FromPort=int(self.rs_db_port),
                    ToPort=int(self.rs_db_port)
                )
            except Exception as e:
                print("Exception opening TCP Port for RedShift Cluster {}".format(e))
        except botocore.exceptions.WaiterError as e :
            print("Exception after the wait with message {}".format(e))

    def delete_redshift_cluster(self):
        self.delete_redshift_role()
        redshift_client = boto3.client('redshift', 
                            aws_access_key_id=self.access_key,
                            aws_secret_access_key= self.secret_key, 
                            region_name=self.region)
        redshift_client.delete_cluster(ClusterIdentifier=self.rs_cluster_identifier, SkipFinalClusterSnapshot=True)
        waiter = redshift_client.get_waiter('cluster_deleted')
        try:
            waiter.wait(ClusterIdentifier=self.rs_cluster_identifier,
                                WaiterConfig={
                                'Delay': 60,
                                'MaxAttempts': 5
                                })
            print("Out of wait check status in console")
        except botocore.exceptions.WaiterError as e :
            print("Exception occured while deleting the Redshift Cluster {}".format(e))

    def delete_redshift_role(self):
        '''
        Function to detach the role policy and the delte the redshift IAM Role
        '''
        try:
            iam = boto3.client('iam',  
                                aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_key) 
            iam.detach_role_policy(RoleName=self.iam_role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
            iam.delete_role(RoleName=self.iam_role_name)
            print("Redshift Role Deleted --->>>>")
        except Exception as e:
            print("Exception occured while deleting the Redshift Role {}".format(e))

    def create_redshift_iam_role(self):
        '''
            Function performs the following
            Initialize boto3 iam 
            Create IAM Role for Redshift service to access other AWS Service
            Attach a Readonly S3 access policy to the newly created role
        '''
        try:
            iam = boto3.client('iam',  
                            aws_access_key_id=self.access_key,
                            aws_secret_access_key=self.secret_key) 
            response = iam.create_role(
                Path="/",
                RoleName=self.iam_role_name,
                Description="Allows Redshift clusters to call AWS Services on your behalf",
                AssumeRolePolicyDocument=json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                            "Service": "redshift.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                ]}))
            print(response)
            print("Successfully created the role with ARN {}".format(response['Role']['Arn']))
            iam.attach_role_policy(RoleName=self.iam_role_name,
                                   PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
            )
            param_dict = defaultdict()
            param_dict['config_identifier'] = 'IAM_ROLE'
            param_dict['param_key'] = 'ARN'
            param_dict['param_value'] = response['Role']['Arn']
            self.update_config_file(param_dict)
            print("Successfully attached the policy for role arn {}".format(self.iam_role_name))
        except Exception as e:
            print(e)




def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    awsIAC = AwsIac(config)
    
    arg_count = len(sys.argv)
    if arg_count > 1 :
        option = sys.argv[1]
        print("Command Line Option {}".format(option))
        if option == "delete":
            awsIAC.delete_redshift_cluster()
        else:
            awsIAC.create_redshift_cluster()
    else:
        awsIAC.create_redshift_cluster()

if __name__ == "__main__":
    main()