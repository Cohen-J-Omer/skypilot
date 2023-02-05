"""IBM cloud adaptors"""

from sky import sky_logging
import yaml
import ibm_cloud_sdk_core
import ibm_vpc
from ibm_platform_services import GlobalSearchV2, GlobalTaggingV1
from ibm_botocore.client import Config
import ibm_boto3
import os

CREDENTIAL_FILE = '~/.ibm/credentials.yaml'
logger = sky_logging.init_logger(__name__)

def read_credential_file():
    with open(os.path.expanduser(CREDENTIAL_FILE), encoding='utf-8') as f:
        return yaml.safe_load(f)

def get_api_key():
        return read_credential_file()['iam_api_key']

def get_hmac_keys():
    cred_file = read_credential_file()
    return cred_file['access_key_id'], cred_file['secret_access_key']

def get_storage_instance_id():
        return read_credential_file()['cos_instance_id']
    
def get_authenticator():
    return ibm_cloud_sdk_core.authenticators.IAMAuthenticator(
        get_api_key())

def client(**kwargs):
    """returns ibm vpc client"""

    try:
        vpc_client = ibm_vpc.VpcV1(version='2022-06-30',
                                   authenticator=get_authenticator())
        if kwargs.get('region'):
            vpc_client.set_service_url(
                f'https://{kwargs["region"]}.iaas.cloud.ibm.com/v1')
    except Exception:
        logger.error('No registered API key found matching specified value')
        raise

    return vpc_client  # returns either formerly or newly created client


def search_client():
    return GlobalSearchV2(authenticator=get_authenticator())

def tagging_client():
    return GlobalTaggingV1(authenticator=get_authenticator())
def get_cos_client(region: str = 'us-east'):
    return ibm_boto3.client(service_name='s3',
                        ibm_api_key_id=get_api_key(),
                        config=Config(signature_version='oauth'),
                        endpoint_url=f'https://s3.{region}.cloud-object-storage.appdomain.cloud')

def get_cos_resource(region: str = 'us-east'):
    return ibm_boto3.resource('s3',ibm_api_key_id=get_api_key(),
                        config=Config(signature_version='oauth'),
                        endpoint_url=f'https://s3.{region}.cloud-object-storage.appdomain.cloud',
                        ibm_service_instance_id = get_storage_instance_id())


