import json
import subprocess
import datetime
import argparse
import logging
from pathlib import Path

# Initialize logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Module selection variables
get_image_catalog = 1
get_iam = 1
get_replicationmanager = 1
get_ml = 1
get_dw = 1
get_datalake = 1

# Util functions
def create_directory(dir_path):
    path = Path(dir_path)
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Creating directory: {dir_path}")

def run_command(cmd, path):
    logger.info(f"Running command: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            response = json.loads(result.stdout)
            return path, response
        else:
            logger.error(f"Error running: {cmd}")
            return path, None
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running command: {e}")
        return path, None

# Argument parsing
parser = argparse.ArgumentParser(description="Gather CDP Information.")
parser.add_argument("profile", nargs='?', default='default', help="profile in ~/.cdp/credentials to use")
parser.add_argument("output_dir", nargs='?', default='output', help="output directory")
args = parser.parse_args()

if __name__ == "__main__":
    now = datetime.datetime.now()
    date_signature = now.strftime("%d_%m_%Y_%H_%M_%S")
    data = {}
    results = {}
    profile_string = f"--profile {args.profile}"
    output_dir = args.output_dir
    output_dir = output_dir + "_" + date_signature
    create_directory(f'{output_dir}')
    output_filename = f"{output_dir}/cdp_info.json"


    # Create a file handler and set the level to INFO
    file_handler = logging.FileHandler(f'{output_dir}/cdp_info.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    # Machine learning
    results['ml'] = {}
    ml = results['ml']
    if get_ml == 1:
        section = "ml"
        sub_command = "list-workspaces"
        params = ""
        path = "workspaces"
        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
        _, response = run_command(cmd, path)
        if response:
            ml['workspaces'] = []
            for entry in response['workspaces']:
                sub_command = "describe-workspace"
                params = f"--environment-name {entry['environmentName']} --workspace-name {entry['instanceName']}"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response1 = run_command(cmd, path)
                if response1:
                    ml['workspaces'].append(response1['workspace'])

    # Datalake
    results['datalake'] = {}
    dl = results['datalake']
    if get_datalake == 1:
        section = "datalake"
        sub_command = "list-datalakes"
        params = ""
        path = "workspaces"
        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
        _, response = run_command(cmd, path)
        if response:
            dl['datalakes'] = []
            for entry in response['datalakes']:
                sub_command = "describe-datalake"
                params = f"--datalake-name {entry['datalakeName']}"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response1 = run_command(cmd, path)
                if response1:
                    dl['datalakes'].append(response1['datalake'])

    # Data warehouse
    results['dw'] = {}
    dw = results['dw']
    if get_dw == 1:
        section = "dw"
        sub_command = "list-clusters"
        params = ""
        path = "clusters"
        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
        _, response = run_command(cmd, path)
        if response:
            dw['clusters'] = []
            for entry in response['clusters']:
                sub_command = "describe-cluster"
                params = f" --cluster-id {entry['id']}"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response1 = run_command(cmd, path)
                if response1:
                    dw['clusters'].append(response1['cluster'])

            # Get data visualizations
            for entry in dw['clusters']:
                entry['data_visualizations'] = []
                sub_command = "list-data-visualizations"
                params = f" --cluster-id {entry['id']}"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response1 = run_command(cmd, path)
                if response1:
                    entry['data_visualizations'].append(response1['dataVisualizations'])

            # Get cluster users
            for entry in dw['clusters']:
                entry['users'] = []
                sub_command = "list-users"
                params = f" --cluster-id {entry['id']}"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response1 = run_command(cmd, path)
                if response1:
                    entry['users'].append(response1['users'])

            # Get cluster users
            for entry in dw['clusters']:
                entry['vws'] = []
                sub_command = "list-vws"
                params = f" --cluster-id {entry['id']}"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response1 = run_command(cmd, path)
                if response1:
                    entry['vws'].append(response1['vws'])

    # Replication manager
    results['replicationmanager'] = {}
    rm = results['replicationmanager']
    if get_replicationmanager == 1:
        section = "replicationmanager"
        sub_command = "list-clusters"
        params = ""
        path = "clusters"
        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
        _, response = run_command(cmd, path)
        if response:
            rm[path] = response['clusters']

    if get_replicationmanager == 1:
        section = "replicationmanager"
        sub_command = "list-policies"
        params = ""
        path = "policies"
        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
        _, response = run_command(cmd, path)
        if response:
            rm[path] = response['policies']

    # Get imagecatalog list custom-catalogs
    if get_image_catalog == 1:
        section = "imagecatalog"
        sub_command = "list-custom-catalogs"
        params 
        path = "image_catalogs"
        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
        _, response = run_command(cmd, path)
        if response:
            results[path] = response['imageCatalogs']
    # Get IAM list access keys
    if get_iam == 1:
        section = "iam"
        sub_command = "list-access-keys"
        params = ""
        path = "access_keys"
        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
        _, response = run_command(cmd, path)
        if response:
            results[path] = response['accessKeys']

        # Get IAM list groups
        section = "iam"
        sub_command = "list-groups"
        params = ""
        path = "groups"
        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
        _, response = run_command(cmd, path)
        if response:
            results[path] = response['groups']

            # Get IAM list groups assigned resource-roles
            section = "iam"
            sub_command = "list-group-assigned-resource-roles"
            path = "assigned_resource_roles"
            for group in results['groups']:
                params = f"--group-name {group['groupName']}"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response = run_command(cmd, path)
                if response:
                    group[path] = response['resourceAssignments']

            # Get IAM list groups roles
            section = "iam"
            sub_command = "list-group-assigned-roles"
            path = "assigned_roles"
            for group in results['groups']:
                params = f"--group-name {group['groupName']}"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response = run_command(cmd, path)
                if response:
                    group[path] = response['roleCrns']

            # Get IAM list group members
            section = "iam"
            sub_command = "list-group-members"
            path = "members"
            for group in results['groups']:
                params = f"--group-name {group['groupName']}"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response = run_command(cmd, path)
                if response:
                    group[path] = response['memberCrns']

            # Get IAM list users
            section = "iam"
            sub_command = "list-users"
            params = ""
            path = "users"
            cmd = f"cdp {profile_string} {section} {sub_command} {params}"
            _, response = run_command(cmd, path)
            if response:
                results[path] = response['users']

                # Get IAM list groups for user
                section = "iam"
                sub_command = "list-groups-for-user"
                path = "assigned_groups"
                for user in results['users']:
                    params = f"--user-id {user['userId']}"
                    cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                    _, response = run_command(cmd, path)
                    if response:
                        user[path] = response['groupCrns']

                # Get IAM list machine users
                section = "iam"
                sub_command = "list-machine-users"
                params = ""
                path = "machine_users"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response = run_command(cmd, path)
                if response:
                    results[path] = response['machineUsers']

                    # Get IAM list groups for machine user
                    section = "iam"
                    sub_command = "list-groups-for-machine-user"
                    path = "assigned_groups"
                    for user in results['machine_users']:
                        params = f"--machine-user-name {user['machineUserName']}"
                        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                        _, response = run_command(cmd, path)
                        if response:
                            user[path] = response['groupCrns']

                    # Get IAM list roles
                    section = "iam"
                    sub_command = "list-roles"
                    params = ""
                    path = "roles"
                    cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                    _, response = run_command(cmd, path)
                    if response:
                        results[path] = response['roles']

    # Get audit_credentials
    section = "environments"
    sub_command = "list-audit-credentials"
    params = ""
    path = "audit_credentials"
    cmd = f"cdp {profile_string} {section} {sub_command} {params}"
    _, response = run_command(cmd, path)
    if response:
        results[path] = response['credentials']

    # Get proxy configs
    section = "environments"
    sub_command = "list-proxy-configs"
    params = ""
    path = "proxy_configs"
    cmd = f"cdp {profile_string} {section} {sub_command} {params}"
    _, response = run_command(cmd, path)
    if response:
        results[path] = response['proxyConfigs']

    # Get list of environments
    section = "environments"
    sub_command = "list-environments"
    params = ""
    path = "/listenvironments/"
    cmd = f"cdp {profile_string} {section} {sub_command} {params}"
    _, response = run_command(cmd, path)
    if response:
        data[path] = response

    # Get details of each environment
    temp = []
    results['environments'] = []
    for entry in data[path]['environments']:
        ename = entry['environmentName']
        params = f"--environment-name {ename}"
        sub_command = "describe-environment"
        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
        _, response = run_command(cmd, path)
        if response:
            results['environments'].append(response['environment'])

    # get opdbs
    for entry in results['environments']:
        ename = entry['environmentName']
        params = f"--environment-name {ename}"
        section = "opdb"
        sub_command = "list-databases"
        cmd = f"cdp {profile_string} {section} {sub_command} {params}"
        _, response = run_command(cmd, path)
        if response:
            entry['databases'] = []
            for subentry in response['databases']:
                sub_command = "describe-database"
                params = f"--environment-name {ename} --database-name {subentry['databaseName']}"
                cmd = f"cdp {profile_string} {section} {sub_command} {params}"
                _, response1 = run_command(cmd, path)
                if response1:
                    entry['databases'].append(response1['databaseDetails'])

    with open(output_filename, "w") as json_file:
        json.dump(results, json_file, indent=4)
        logger.info(f"Data written to {output_filename}")
        print(f"Data written to {output_filename}")
