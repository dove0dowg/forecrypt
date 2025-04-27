# libs
from clickhouse_driver import Client
import os
import subprocess
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import logging
import base64
import time
from typing import Any
# modules
from config.config_system import CH_DB_CONFIG
# logger
logger = logging.getLogger(__name__)
# ---------------------------------------------------------
load_dotenv()
# ---------------------------------------------------------
# [Update config] (by environmental variables)
# ---------------------------------------------------------
def update_ch_config(clickhouse_config: dict[str, Any]) -> dict[str, Any]:
    """Update Clickhouse config from ernvironment, if present"""
    active_config = clickhouse_config.copy()
    active_config.update({
        'database': os.getenv('FORECRYPT_CH_DB_NAME', active_config['database']),
        'table': os.getenv('FORECRYPT_CH_DB_TABLE', active_config['table']),
        'user': os.getenv('FORECRYPT_CH_DB_USER', active_config['user']),
        'password': os.getenv('FORECRYPT_CH_DB_PASS', active_config['password']),
        'host': os.getenv('FORECRYPT_CH_DB_HOST', active_config['host']),
        'port': int(os.getenv('FORECRYPT_CH_DB_PORT', active_config['port'])),
        'http_port': int(os.getenv('FORECRYPT_CH_HTTP_PORT', active_config['http_port'])),
        'interserver_port': int(os.getenv('FORECRYPT_CH_INTERSERVER_PORT', active_config['interserver_port'])),
        'container_name': os.getenv('FORECRYPT_CH_DB_CONTAINER', active_config['container_name']),
        'default_user_xml_path': os.getenv('FORECRYPT_CH_DB_DEFAULT_USER_XML', active_config['default_user_xml_path']),
        'users_xml_path': os.getenv('FORECRYPT_CH_DB_USERS_XML', active_config['users_xml_path']),
        'db_data_wsl_dir': os.getenv('CLICKHOUSE_WSL_DATA_DIR', active_config['db_data_wsl_dir']),
        'config_wsl_dir': os.getenv('CLICKHOUSE_WSL_CONFIG_DIR', active_config['config_wsl_dir']),
    })
    return active_config
# ---------------------------------------------------------
# [Run container] (wsl docker...)
# ---------------------------------------------------------
def ensure_docker_running() -> bool:
    """
    Ensures the Docker daemon is running in WSL.

    This function checks if the Docker daemon is active by running `docker ps`.
    If the daemon is not running, it attempts to start it using `service docker start`.
    First, it tries starting without root privileges, then retries with `wsl -u root`
    if the initial attempt fails.

    Returns:
        bool: True if Docker is running or successfully started, False otherwise.

    Logs:
        - INFO: When Docker is already running or successfully started.
        - WARNING: If Docker is not running initially.
        - ERROR: If all attempts to start Docker fail.
        - EXCEPTION: If an unexpected error occurs.
    """
    try:
        # first check if Docker is already running
        check_cmd = "wsl docker ps"
        result = subprocess.run(check_cmd, shell=True,
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE,
                              text=True)
        
        if result.returncode == 0:
            logger.info("Docker daemon is already running")
            return True
            
        logger.warning("Docker daemon not running, attempting to start...")
        
        # try without root first (works on some WSL installations)
        start_cmd = "wsl service docker start"
        result = subprocess.run(start_cmd, shell=True,
                              capture_output=True, 
                              text=True, 
                              timeout=10)
        
        # if still not running, try with root
        if result.returncode != 0:
            start_cmd = "wsl -u root service docker start"
            result = subprocess.run(start_cmd, shell=True,
                                  capture_output=True,
                                  text=True,
                                  timeout=10)
        
        if result.returncode == 0:
            logger.info("Docker daemon started successfully")
            return True
            
        logger.error(f"Failed to start Docker: {result.stderr}")
        return False
        
    except Exception as e:
        logger.exception(f"Error starting Docker: {str(e)}")
        return False

def remove_clickhouse_container(container_name: str):
    """
    Forcefully removes a ClickHouse Docker container if it exists.

    This function attempts to remove the specified ClickHouse container using `docker rm -f`.
    If the container does not exist, it logs a debug message instead of raising an error.

    Args:
        container_name (str): The name of the ClickHouse container to remove.

    Logs:
        - DEBUG: When attempting to remove the container or if it does not exist.
        - INFO: If the container was successfully removed.
        - WARNING: If an error occurs during removal.
    """
    try:
        logger.debug(f"Attempting to remove container: {container_name}")
        result = subprocess.run(
            f"wsl docker rm -f {container_name}",
            shell=True,
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            logger.info(f"Removed existing container: {container_name}")
        else:
            logger.debug(f"No container to remove: {container_name}")
    except Exception as e:
        logger.warning(f"Error removing container: {str(e)}")

def init_volumes_and_config(clickhouse_config: dict):
    """
    Initializes ClickHouse volumes and copies configuration files with corrected syntax.

    This function ensures the necessary directories exist and then uses a temporary
    Docker container to copy ClickHouse configuration files to the specified volume.
    It also adjusts ownership to match ClickHouse’s default user (UID 101, GID 101).

    Args:
        clickhouse_config (dict): A dictionary containing configuration paths, including:
            - 'config_wsl_dir' (str): The WSL directory where ClickHouse configs will be stored.

    Logs:
        - INFO: When configuration files are successfully copied.
        - ERROR: If a fatal error occurs during execution.

    Raises:
        SystemExit: If the operation fails due to a subprocess error.
    """
    try:
        # create config directory inside wsl
        subprocess.run(
            f"wsl mkdir -p {clickhouse_config['config_wsl_dir']}", 
            shell=True, 
            check=True
        )
        
        # copy default Clickhouse configuration file  
        subprocess.run(
            'wsl docker run --rm '
            f'-v {clickhouse_config["config_wsl_dir"]}:/target '
            'clickhouse/clickhouse-server '
            '/bin/sh -c "'
            'cp -r /etc/clickhouse-server/* /target && '
            'chown -R 101:101 /target"', 
            shell=True, 
            check=True
        )
        
        logger.info("Default configuration files copied from temporary Clickhouse container")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"FATAL ERROR while copying config from temporary Clickhouse container: {e.stderr}")
        raise SystemExit(1)

def initial_start_clickhouse_container(clickhouse_config: dict[str, Any]) -> bool:
    """
    Starts the ClickHouse container with extended logging. Returns True if the container starts successfully, 
    and False if it encounters an error.

    This function:
    - Checks for required configuration parameters (`port`, `http_port`, `interserver_port`, 
      `container_name`, `db_data_wsl_dir`, `config_wsl_dir`).
    - Prepares and executes the Docker run command to start the container.
    - Mounts volumes for database data and configuration from the WSL filesystem into the container.
    - Logs the container's startup process, capturing both standard and error outputs.
    - Provides diagnostic information if the container fails to start, including container logs and status.

    Args:
        clickhouse_config (dict): A dictionary containing the configuration for the ClickHouse container.
            Required keys include:
            - 'port' (int): The port to map the ClickHouse server's native port.
            - 'http_port' (int): The HTTP port for ClickHouse.
            - 'interserver_port' (int): The interserver port.
            - 'container_name' (str): The name of the Docker container.
            - 'db_data_wsl_dir' (str): Path to the database data directory on the WSL filesystem.
            - 'config_wsl_dir' (str): Path to the configuration directory on the WSL filesystem.

    Returns:
        bool: True if the container starts successfully, False otherwise.
    """
    try:
        logger.info("Starting ClickHouse container setup...")
        logger.debug(f"Using config: {clickhouse_config}")

        # check necessary config dictionary keys
        required_keys = ['port', 'http_port', 'interserver_port', 'container_name',
                       'db_data_wsl_dir', 'config_wsl_dir']
        for key in required_keys:
            if key not in clickhouse_config:
                logger.error(f"Missing required clickhouse_config key: {key}")
                return False

        # prepare cmd command with volumes mounting
        run_clickhouse_cmd = (
            f"wsl docker run -d "
            f"--name {clickhouse_config['container_name']} "
            f"-v {clickhouse_config['db_data_wsl_dir']}:/var/lib/clickhouse "
            f"-v {clickhouse_config['config_wsl_dir']}:/etc/clickhouse-server "
            f"-p {clickhouse_config['port']}:9000 "
            f"-p {clickhouse_config['http_port']}:8123 "
            f"-p {clickhouse_config['interserver_port']}:9009 "
            f"--ulimit nofile=262144:262144 "
            f"clickhouse/clickhouse-server"
        )

        logger.debug(f"Executing command: {run_clickhouse_cmd}")
        
        # execute cmd command
        result = subprocess.run(
            run_clickhouse_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )

        # logging through resuilt 
        if result.returncode == 0:
            logger.info(f"Container {clickhouse_config['container_name']} started successfully")
            logger.debug(f"Container ID: {result.stdout.strip()}")
            return True
        else:
            logger.error(f"Failed to start container. Exit code: {result.returncode}")
            logger.error(f"Error output: {result.stderr}")
            
            # additional diagnostics
            logger.info("Running diagnostic commands...")
            subprocess.run("wsl docker ps -a", shell=True)
            subprocess.run(f"wsl docker logs {clickhouse_config['container_name']}", shell=True)
            return False

    except subprocess.TimeoutExpired:
        logger.error("Command timed out after 30 seconds")
        return False
        
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        return False
# final combined [Run container] function
def clickhouse_container_forced_install(clickhouse_config: dict[str, Any]):
    """
    Forces the installation and setup of the ClickHouse container. The process includes:
    - Ensuring Docker is running.
    - Removing any existing ClickHouse container with the specified name.
    - Initializing necessary volumes and configuration files.
    - Starting the ClickHouse container with the provided configuration.

    This function performs the following steps:
    1. Verifies if Docker is running by invoking the `ensure_docker_running()` function.
    2. Removes any pre-existing ClickHouse container using the `remove_clickhouse_container()` function.
    3. Initializes volumes and configuration files by calling the `init_volumes_and_config()` function.
    4. Attempts to start the ClickHouse container with the `initial_start_clickhouse_container()` function.
    
    If any step fails (Docker not running, container removal, volume initialization, or container start),
    the function logs the error and returns `False`. If all steps are successful, it returns `True`.

    Args:
        clickhouse_config (dict): A dictionary containing the configuration for the ClickHouse container.
            Required keys include:
            - 'container_name' (str): The name of the Docker container.
            - Other keys necessary for volume initialization and container setup are expected.

    Returns:
        bool: `True` if ClickHouse container was successfully installed and started, `False` otherwise.
    """

    try:

        logger.info("=== Starting Clickhouse Install and Deploy ===")
        
        if not ensure_docker_running():
            logger.error("Docker is not running! Aborting.")
            return False    
        
        remove_clickhouse_container(clickhouse_config["container_name"])
        init_volumes_and_config(clickhouse_config)

        if not initial_start_clickhouse_container(clickhouse_config):
            logger.error("Coudn't start Clickhouse docker container! Aborting.")
            return False         

        #time.sleep(5)  # giving time for ClickHouse to start

        return True
    
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        return False
# ---------------------------------------------------------
# [Add admin] (functions to add admin user in a secure way)
# ---------------------------------------------------------
def get_users_xml(clickhouse_config: dict[str, Any]):
    """
    Retrieves the ClickHouse users.xml file from the running Docker container.

    This function executes a command inside the specified ClickHouse container to read 
    the users.xml configuration file and return its contents as a string.

    Returns:
        str | None: The content of the users.xml file if successfully retrieved, 
        or None if an error occurs.

    Raises:
        Logs an error if the command fails or an exception occurs.
    """
    try:
        # get the file path and container name from the configuration
        container_name = clickhouse_config['container_name']
        users_xml_path = clickhouse_config['users_xml_path']
        # forming a command to get a file from a container
        cmd_command = f"wsl docker exec {container_name} cat {users_xml_path}"
        # executing cmd command
        result = subprocess.run(cmd_command, capture_output=True, text=True, shell=True)
        if result.returncode == 0:
            return result.stdout  # return XML-data
        else:
            logger.error(f"Failed to retrieve file {users_xml_path} from container.")
            return None
    except Exception as e:
        logger.error(f"Error reading users.xml: {e}")
        return None
    
def modify_users_xml(xml_data, clickhouse_config: dict[str, Any]):
    """
    Modifies the ClickHouse users.xml file by updating default user settings.

    This function parses the XML configuration, updates specific permissions for 
    the default user, and sets a new password. If the required elements do not exist, 
    they are created.

    Args:
        xml_data (str): The original XML content as a string.
        clickhouse_config (dict[str, Any]): Configuration containing the new password.

    Returns:
        tuple(str, str) | None: A tuple containing the original and modified XML content as strings, 
        or None if an error occurs.

    Raises:
        Logs errors if the XML parsing fails or required elements are missing.
    """
    try:
        root = ET.fromstring(xml_data)

        new_password_for_default = clickhouse_config['password']
        
        # look for the <default> element and change the necessary settings
        default_user = root.find(".//users/default")
        if default_user is not None:
            # change settings
            for param in ["access_management", "named_collection_control", "show_named_collections", "show_named_collections_secrets"]:
                elem = default_user.find(f".//{param}")
                if elem is None:
                    # create element if doesn't exist
                    elem = ET.SubElement(default_user, param)
                    logger.info(f"Created new element {param} in <default>")
                elem.text = "1"

            # change password (it doesn't matter much cause password setting is overridden by default_user.xml)
            password_elem = default_user.find("password")
            if password_elem is None:
                password_elem = ET.SubElement(default_user, "password")
                logger.info("Created new element 'password' in <default>")
            password_elem.text = new_password_for_default
            logger.info("Password for <default> successfully changed")
        else:
            logger.error("Element doesn't exist in <default> block in XML-file")

        # save modified XML back to string
        modified_users_xml = ET.tostring(root, encoding='unicode')
        # set variable to return original data
        original_users_xml = xml_data
        
        # return both original and modified XML
        return original_users_xml, modified_users_xml

    except ET.ParseError as e:
        logger.error(f"XML-parsing error: {e}")
        return None
    except Exception as e:
        logger.error(f"XML-changing error: {e}")
        return None
    
def unmodify_users_xml(xml_data):
    """
    Reverts modifications in the ClickHouse users.xml file by resetting default user settings.

    This function parses the XML configuration and disables specific permissions for 
    the default user by setting them to "0". If the required elements do not exist, 
    they are created.

    Args:
        xml_data (str): The original XML content as a string.

    Returns:
        str | None: The modified XML content as a string, or None if an error occurs.

    Raises:
        Logs errors if the XML parsing fails or required elements are missing.
    """
    try:
        root = ET.fromstring(xml_data)

        # find the <default> user element
        default_user = root.find(".//users/default")

        if default_user is not None:
            # update specific settings by setting them to "0"
            for param in ["access_management", "named_collection_control", "show_named_collections", "show_named_collections_secrets"]:
                elem = default_user.find(f".//{param}")
                if elem is None:
                    # create the element if it does not exist
                    elem = ET.SubElement(default_user, param)
                    logger.info(f"Created new element {param} in <default>")
                elem.text = "0"
        else:
            logger.error("Element <default> not found in xml")

        # return the modified xml as a string
        return ET.tostring(root, encoding='unicode')

    except ET.ParseError as e:
        logger.error(f"XML parsing error: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error modifying xml: {e}")
        return None
    
def update_users_xml(modified_xml, clickhouse_config: dict[str, Any]) -> bool:
    """
    Updates the users.xml file inside a running ClickHouse container.

    This function encodes the provided XML data in Base64 to avoid issues with special characters 
    and transmits it to the container via a shell command. After writing the file, it verifies 
    its existence to ensure the update was successful.

    Args:
        modified_xml (str): The modified XML content to be written.
        clickhouse_config (dict[str, Any]): Configuration dictionary containing at least:
            - container_name (str): The name of the ClickHouse container.

    Logs:
        - DEBUG: Executed commands, process outputs, and diagnostics.
        - ERROR: Failure reasons if the operation is unsuccessful.

    Raises:
        Exception: Logs critical errors encountered during execution.
    """
    try:
        container_name = clickhouse_config['container_name']
        users_xml_path = "/etc/clickhouse-server/users.xml"

        # encode xml in base64 to avoid issues with quotes and special characters
        encoded_xml = base64.b64encode(modified_xml.encode("utf-8")).decode("utf-8")

        # command to decode and save the file inside the container
        docker_cmd = (
            f'wsl docker exec {container_name} sh -c '
            f'"echo \'{encoded_xml}\' | base64 --decode > {users_xml_path}"'
        )

        logger.debug(f"Executing command: {docker_cmd}")

        # execute the command
        result = subprocess.run(
            docker_cmd,
            shell=True,
            capture_output=True,
            text=True
        )

        # log full diagnostics
        logger.debug(f"Return code: {result.returncode}")
        logger.debug(f"stdout: {result.stdout}")
        logger.debug(f"stderr: {result.stderr}")

        if result.returncode == 0:
            # additional check to confirm the file was created
            check_cmd = f'wsl docker exec {container_name} sh -c "ls -la {users_xml_path}"'
            check_result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)

            if check_result.returncode == 0:
                logger.info(f"File users.xml created and updated by encoded base64 XML-data")
                logger.debug(f"File {users_xml_path} created and updated by encoded base64 XML-data : {check_result.stdout}")
                logger.debug(f"File successfully created: {check_result.stdout}")
                logger.debug("File successfully written using base64 encoding")
                return True
            else:
                logger.error(f"File creation check failed: {check_result.stderr}")
                return False
        else:
            logger.error(f"Error executing command: {result.stderr}")
            return False

    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
        logger.error(f"Exception: {str(e)}")
        return False

def get_default_user_xml(clickhouse_config: dict[str, Any]):
    """
    Retrieves the default-user.xml file from a running ClickHouse container.

    This function executes a command inside the ClickHouse container to read the contents 
    of the default-user.xml file and return it as a string.

    Args:
        clickhouse_config (dict[str, Any]): Configuration dictionary containing:
            - container_name (str): The name of the ClickHouse container.
            - default_user_xml_path (str): The path to the default-user.xml file inside the container.

    Returns:
        str | None: The XML content as a string if successful, otherwise None.

    Logs:
        - ERROR: If the file retrieval fails or an exception occurs.
    """
    try:
        container_name = clickhouse_config['container_name']
        default_user_xml_path = clickhouse_config['default_user_xml_path']
        
        cmd_command = f"wsl docker exec {container_name} cat {default_user_xml_path}"
        result = subprocess.run(cmd_command, capture_output=True, text=True, shell=True)
        if result.returncode == 0:
            return result.stdout
        else:
            logger.error(f"Failed to retrieve file {default_user_xml_path} from container.")
            return None
    except Exception as e:
        logger.error(f"Error reading default-user.xml: {e}")
        return None

def modify_default_user_xml(xml_data):
    """
    Modifies the default-user.xml file by updating network access settings.

    This function ensures that the <default> user in the XML has unrestricted external access 
    by setting the <ip> entry under <networks> to "::/0".

    Args:
        xml_data (str): The original XML content as a string.

    Returns:
        tuple(str, str) | None: A tuple containing the original and modified XML content as strings, 
        or None if an error occurs.

    Logs:
        - INFO: If the network settings are successfully updated.
        - ERROR: If the <default> element is missing or an error occurs.
    """
    try:
        root = ET.fromstring(xml_data)

        # find <default> user element
        default_user = root.find(".//users/default")
        if default_user is not None:
            # find or create <networks> element
            networks = default_user.find("networks")
            if networks is None:
                networks = ET.SubElement(default_user, "networks")

            # remove existing <ip> entries
            for ip in networks.findall("ip"):
                networks.remove(ip)

            # add new <ip> entry allowing external access
            ET.SubElement(networks, "ip").text = "::/0"
            logger.info("Network settings updated: external access enabled")

        else:
            logger.error("Missing <default> element in XML")

        # save modified XML back to string
        modified_default_user_xml = ET.tostring(root, encoding='unicode')
        # set variable to return original data
        original_default_user_xml = xml_data

        # return both original and modified XML
        return original_default_user_xml, modified_default_user_xml

    except ET.ParseError as e:
        logger.error(f"XML parsing error: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while modifying XML: {e}")
        return None

def update_default_user_xml(modified_xml, clickhouse_config: dict[str, Any]) -> bool:
    """
    Updates the default-user.xml file inside the Docker container by decoding and writing the modified XML content.

    This function encodes the modified XML content in base64 to avoid issues with special characters and quotes,
    then decodes and saves it inside the container at the specified path.

    Args:
        modified_xml (str): The modified XML content as a string.
        clickhouse_config (dict): The configuration dictionary containing container details and XML path.

    Logs:
        - DEBUG: Command execution details, return codes, and file creation checks.
        - ERROR: If the command execution or file creation fails.
        - CRITICAL: For unexpected errors during the update process.
    """
    try:
        container_name = clickhouse_config['container_name']
        default_user_xml_path = clickhouse_config['default_user_xml_path']

        # encode XML in base64 to handle special characters
        encoded_xml = base64.b64encode(modified_xml.encode("utf-8")).decode("utf-8")

        # command to decode and save the XML inside the container
        docker_cmd = (
            f'wsl docker exec {container_name} sh -c '
            f'"echo \'{encoded_xml}\' | base64 --decode > {default_user_xml_path}"'
        )

        logger.debug(f"Executing command: {docker_cmd}")
        result = subprocess.run(docker_cmd, shell=True, capture_output=True, text=True)

        # log full diagnostics
        logger.debug(f"Return code: {result.returncode}")
        logger.debug(f"stdout: {result.stdout}")
        logger.debug(f"stderr: {result.stderr}")

        if result.returncode == 0:
            # additional check to confirm the file was created
            check_cmd = f'wsl docker exec {container_name} sh -c "ls -la {default_user_xml_path}"'
            check_result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)

            if check_result.returncode == 0:
                logger.info(f"File default_user.xml created and updated by encoded base64 XML-data")
                logger.debug(f"File {default_user_xml_path} created and updated by encoded base64 XML-data : {check_result.stdout}")
                logger.debug(f"File successfully created: {check_result.stdout}")
                logger.debug("File successfully written using base64 encoding")
                return True
            else:
                logger.error(f"File creation check failed: {check_result.stderr}")
                return False
        else:
            logger.error(f"Error executing command: {result.stderr}")
            return False

    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
        logger.error(f"Exception: {str(e)}")
        return False

def docker_nopassword_check_ch_port(clickhouse_config: dict[str, Any]) -> bool:
    """
    Repeatedly checks if ClickHouse is accessible on port 9000 inside the Docker container.

    This function executes a simple SELECT 1 query via clickhouse-client up to 20 times 
    with a 0.5-second interval, ensuring ClickHouse is running and responding.

    Args:
        clickhouse_config (dict[str, Any]): A dictionary containing container name and password.

    Returns:
        bool: True if ClickHouse responds successfully within the attempts, False otherwise.

    Logs:
        - INFO: Each attempt result (success or failure).
        - DEBUG: Detailed error messages if a failure occurs.
        - ERROR: If all attempts fail.
    """
    check_cmd = (
        f"wsl docker exec {clickhouse_config['container_name']} "
        f"clickhouse-client --user=default "
        f"-q 'SELECT 1'"
    )
    
    for attempt in range(1, 21): 
        try:
            logger.debug(f"Executing port check attempt {attempt}: {check_cmd}")
            result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"ClickHouse port 9000 is open and responding (attempt {attempt}/20).")
                return True
            else:
                logger.debug(f"ClickHouse port check failed (attempt {attempt}/20).")
                logger.debug(f"Error details: {result.stderr.strip()}")

        except Exception as e:
            logger.info(f"Unexpected error while checking ClickHouse port (attempt {attempt}/20).")
            logger.debug(f"Exception details: {e}")

        time.sleep(0.5)

    logger.error("ClickHouse port 9000 did not respond after 20 attempts.")
    return False

def docker_password_check_ch_port(clickhouse_config: dict[str, Any]) -> bool:
    """
    Repeatedly checks if ClickHouse is accessible on port 9000 inside the Docker container.

    This function executes a simple SELECT 1 query via clickhouse-client up to 20 times 
    with a 0.5-second interval, ensuring ClickHouse is running and responding.

    Args:
        clickhouse_config (dict[str, Any]): A dictionary containing container name and password.

    Returns:
        bool: True if ClickHouse responds successfully within the attempts, False otherwise.

    Logs:
        - INFO: Each attempt result (success or failure).
        - DEBUG: Detailed error messages if a failure occurs.
        - ERROR: If all attempts fail.
    """
    check_cmd = (
        f"wsl docker exec {clickhouse_config['container_name']} "
        f"clickhouse-client --user=default --password={clickhouse_config['password']} "
        f"-q 'SELECT 1'"
    )
    
    for attempt in range(1, 21):
        try:
            logger.debug(f"Executing port check attempt {attempt}: {check_cmd}")
            result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"ClickHouse port 9000 is open and responding (attempt {attempt}/20).")
                return True
            else:
                logger.debug(f"ClickHouse port check failed (attempt {attempt}/20).")
                logger.debug(f"Error details: {result.stderr.strip()}")

        except Exception as e:
            logger.info(f"Unexpected error while checking ClickHouse port (attempt {attempt}/20).")
            logger.debug(f"Exception details: {e}")

        time.sleep(0.5)

    logger.error("ClickHouse port 9000 did not respond after 20 attempts.")
    return False

def docker_reload_clickhouse_config(clickhouse_config: dict[str, Any]) -> bool:
    """
    Reloads the ClickHouse configuration and resets the default password if needed.

    This function attempts to reload the ClickHouse configuration by executing a 
    command via Docker. If the reload fails, it attempts to reset the default password 
    by removing the default password configuration file.

    Args:
        clickhouse_config (dict[str, Any]): A dictionary containing the container name and password.

    Returns:
        bool: True if the configuration is reloaded successfully or the password is reset.
              False if an error occurs during the reload or password reset process.

    Logs:
        - INFO: When the configuration is successfully reloaded or the default password is reset.
        - ERROR: When the reload fails or any error occurs during the process.
    """
    try:
        reload_cmd = (
            f"wsl docker exec {clickhouse_config['container_name']} "
            f"clickhouse-client --user=default --password={clickhouse_config['password']} "
            f"-q 'SYSTEM RELOAD CONFIG'"
        )
        logger.debug(f'Executing command: {reload_cmd}')
        subprocess.run(reload_cmd, shell=True, check=True)

        logger.info("ClickHouse configuration reloaded successfully (through docker).")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to reload config: {e.stderr}")
        # Trying to reset the default password
        reset_cmd = (
            f"wsl docker exec {clickhouse_config['container_name']} "
            "rm -f /etc/clickhouse-server/users.d/default-password.xml"
        )
        subprocess.run(reset_cmd, shell=True)
        logger.info("Tried to reset default password")
        return False

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def docker_create_admin_user(clickhouse_config: dict) -> bool:
    """
    Creates an admin user in the ClickHouse database via Docker exec by executing SQL commands.

    This function generates SQL commands to create a user and grant privileges, encodes them in base64 to avoid issues 
    with special characters, and then executes the commands inside the container using docker exec.

    Args:
        clickhouse_config (dict): The configuration dictionary containing container details, user credentials, and other parameters.

    Returns:
        bool: True if the admin user was successfully created, False otherwise.

    Logs:
        - INFO: When the admin user is successfully created.
        - ERROR: If the command execution fails or encounters an error.
        - CRITICAL: If an unexpected exception occurs during the process.
    """
    try:
        container_name = clickhouse_config['container_name']
        admin_user = clickhouse_config['user']
        admin_pass = clickhouse_config['password']
        
        # prepare SQL commands
        sql_commands = f"""
        CREATE USER IF NOT EXISTS {admin_user}
        IDENTIFIED WITH sha256_password BY '{admin_pass}';
        
        GRANT ALL PRIVILEGES ON *.* TO {admin_user} WITH GRANT OPTION;
        """  # simplified to basic privileges
        
        # encode SQL commands in base64
        encoded_commands = base64.b64encode(sql_commands.encode()).decode()
        
        # execute via docker exec
        cmd = (
            f'wsl docker exec {container_name} '
            f'sh -c "echo \'{encoded_commands}\' | '
            f'base64 --decode | '
            f'clickhouse-client --user=default --password=clickhouse_password -mn"'
        )
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"Admin user {admin_user} successfully created via docker exec")
            return True
            
        logger.error(f"Error: {result.stderr}")
        return False
        
    except Exception as e:
        logger.error(f"Failure: {str(e)}")
        return False
# final combined [Add admin] function
def configure_clickhouse_user_permissions(clickhouse_config: dict[str, Any]) -> bool:
    """
    Configures ClickHouse user permissions by modifying the users.xml and default_user.xml files 
    to allow the default user to create other users, then reloads the ClickHouse configuration 
    and creates an admin user. Finally, it restores the original users.xml and default_user.xml files.

    This function performs the following steps:
        1. Retrieves the current `default_user.xml` file.
        2. Modifies it to grant permission to the default user.
        3. Updates the `default_user.xml` file in the running ClickHouse container.
        4. Retrieves the current `users.xml` file.
        5. Modifies it to allow the default user to manage other users.
        6. Updates the `users.xml` file in the running ClickHouse container.
        7. Reloads the ClickHouse configuration to apply changes.
        8. Creates an admin user in the container.
        9. Restores the original `default_user.xml` and `users.xml` files for security.
    
    Args:
        clickhouse_config (dict[str, Any]): Configuration dictionary containing container details.

    Returns:
        bool: True if all operations were successful, False otherwise.

    Logs:
        - INFO: If the operations are completed successfully.
        - ERROR: If any step fails during the process.
    """
    try:

        # check Clickhouse port availability
        if not docker_nopassword_check_ch_port(clickhouse_config):
            logger.critical("ClickHouse is not responding. Aborting reload.")
            return False

        # modify default_user.xml
        default_user_xml = get_default_user_xml(clickhouse_config)
        if default_user_xml:
            original_default_user_xml, modified_default_user_xml = modify_default_user_xml(default_user_xml)
            if not update_default_user_xml(modified_default_user_xml, clickhouse_config):
                logger.error("Failed to update default_user.xml in the container.")
                return False
        else:
            logger.error("Failed to retrieve default_user.xml.")
            return False

        # modify users.xml
        users_xml_file = get_users_xml(clickhouse_config)
        if users_xml_file:
            original_users_xml, modified_users_xml = modify_users_xml(users_xml_file, clickhouse_config)
            if not update_users_xml(modified_users_xml, clickhouse_config):
                logger.error("Failed to update users.xml in the container.")
                return False
        else:
            logger.error("Failed to retrieve users.xml.")
            return False

        # check Clickhouse port availability
        if not docker_password_check_ch_port(clickhouse_config):
            logger.critical("ClickHouse is not responding after users.xml and default_user.xml update. Aborting reload.")
            return False
        
        # reload configuration through docker
        docker_reload_clickhouse_config(clickhouse_config)

        if not docker_password_check_ch_port(clickhouse_config):
            logger.critical("ClickHouse is not responding after Clickhouse config reload. Aborting reload.")
            return False        

        # create admin user
        if not docker_create_admin_user(clickhouse_config):
            logger.error("Failed to create admin user.")
            return False
        
        if not docker_password_check_ch_port(clickhouse_config):
            logger.critical("ClickHouse is not responding after creating admin user. Aborting reload.")
            return False   

        # restore users.xml and default_user.xml files for security
        if not update_default_user_xml(original_default_user_xml, clickhouse_config):
            logger.error("Failed to restore original default_user.xml.")
            return False
        if not update_users_xml(original_users_xml, clickhouse_config):
            logger.error("Failed to restore original users.xml.")
            return False

        # All steps successful
        logger.info("ClickHouse user permissions configured and security restored successfully.")
        return True

    except Exception as e:
        logger.error(f"Error during ClickHouse user configuration: {e}")
        return False
# ---------------------------------------------------------
# [Clickhouse client] (create client object for future queries)
# ---------------------------------------------------------
def clickhouse_connection(clickhouse_config: dict[str, Any]):
    """
    Establishes a connection to a ClickHouse database using provided configuration.

    This function attempts to establish a connection using the `Client` from `clickhouse-driver`.
    It retries up to 10 times with 0.3-second intervals if the initial attempts fail.

    Args:
        clickhouse_config (dict[str, Any]): A dictionary containing ClickHouse connection parameters.

    Returns:
        Client | None: The `Client` object if the connection is successful, otherwise `None`.

    Logs:
        - INFO: Successful connection or attempt status.
        - DEBUG: Error details on failure.
    """
    host = clickhouse_config['host']
    port = clickhouse_config['port']
    user = clickhouse_config['user']
    password = clickhouse_config['password']

    for attempt in range(1, 11):
        try:
            client = Client(host=host, port=port, user=user, password=password)
            result = client.execute('SELECT 1')
            logger.debug(f'Successful ClickHouse connection on attempt {attempt}. Result: {result}')
            return client
        except Exception as e:
            logger.info(f'ClickHouse connection attempt {attempt} failed.')
            logger.debug(f'Error details: {e}')
            time.sleep(0.3)
    
    logger.error("Failed to connect to ClickHouse after 10 attempts.")
    return None

def client_check_ch_ready(clickhouse_config: dict[str, Any]) -> bool:
    """
    Repeatedly checks if ClickHouse is ready to accept queries via clickhouse-driver.

    This function attempts to establish a connection and execute a simple SELECT 1 query 
    up to 20 times with a 0.5-second interval, ensuring ClickHouse is operational.

    Args:
        clickhouse_config (dict[str, Any]): A dictionary containing ClickHouse connection parameters.

    Returns:
        bool: True if ClickHouse responds successfully within the attempts, False otherwise.

    Logs:
        - INFO: Each attempt result (success or failure).
        - DEBUG: Detailed error messages if a failure occurs.
        - ERROR: If all attempts fail.
    """
    for attempt in range(1, 21):  
        try:
            client = Client(
                host=clickhouse_config["host"],
                port=clickhouse_config["port"],
                user=clickhouse_config["user"],
                password=clickhouse_config["password"],
            )
            result = client.execute("SELECT 1")

            if result == [(1,)]:
                logger.info(f"ClickHouse is ready and accepting queries (attempt {attempt}/20).")
                return True
            else:
                logger.info(f"ClickHouse responded unexpectedly (attempt {attempt}/20): {result}")

        except Exception as e:
            logger.info(f"ClickHouse is not ready yet (attempt {attempt}/20).")
            logger.debug(f"Exception details: {e}")

        time.sleep(0.5)

    logger.error("ClickHouse did not become ready after 20 attempts.")
    return False

def client_reload_clickhouse_config(clickhouse_client: Client, clickhouse_config: dict[str, Any]) -> bool:
    """
    Reloads the ClickHouse configuration using the SYSTEM RELOAD CONFIG query.

    This function sends a query to the ClickHouse server to reload the configuration. 
    It uses the established connection to execute the query and check for success.

    Args:
        clickhouse_client (Client): The ClickHouse client object to execute the query with.

    Returns:
        bool: Returns `True` if the configuration reload was successful, `False` otherwise.

    Logs:
        - INFO: Logs the result of the reload attempt, whether successful or failed.
        - ERROR: Logs any exceptions or errors that occur during the reload process.
    
    Example:
        success = client_reload_clickhouse_config(client)
        if success:
            logger.info("Configuration reloaded successfully.")
        else:
            logger.error("Failed to reload configuration.")
    """
    try:

        if not client_check_ch_ready(clickhouse_config):
            logger.critical("ClickHouse is not responding through client. Aborting reload.")
            return False
        
        # Send the reload config query
        clickhouse_client.execute('SYSTEM RELOAD CONFIG')

        # If we reach here, it means the query was successful
        logger.info("ClickHouse configuration reloaded successfully (through client).")
        return True
    except Exception as e:
        # In case of any error, log it and return False
        logger.error(f"Failed to reload configuration: {e}")
        return False

def create_database(clickhouse_client: Client, clickhouse_config: dict[str, Any]) -> bool:
    """
    Creates a database in ClickHouse using the name from config.
    
    :param clickhouse_client: The ClickHouse client object used for connecting to the database.
    :param clickhouse_config: A dictionary containing the configuration, including the 'database' key.
    :return: True if the database was successfully created or already exists, False if there was an error.
    """
    try:
        database_name = clickhouse_config['database']

        # Create database if not exists
        clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        logger.info("Database created successfully.")
        return True

    except Exception as e:
        logger.info(f"Failed to create database: {e}")
        return False

def drop_clickhouse_database(clickhouse_client: Client, clickhouse_config: dict[str, Any]) -> bool:
    """
    Drops a specified database in ClickHouse along with all its contents.

    This function first deletes all tables in the database and then removes the database itself.

    :param clickhouse_client: The ClickHouse client object used for connecting to the database.
    :param clickhouse_config: Configuration dictionary containing connection parameters and database name.
    :return: True if the database and its contents were successfully dropped, False otherwise.
    """
    try:
        database_name = clickhouse_config['database']

        # Check if the database exists by listing databases and checking for the target one
        result = clickhouse_client.execute(f"SHOW DATABASES")
        if database_name not in [db[0] for db in result]:
            logger.info(f"Database '{database_name}' does not exist.")
            return True

        # Drop all tables in the database
        tables = clickhouse_client.execute(f"SHOW TABLES FROM {database_name}")

        for table in tables:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {database_name}.{table[0]}")

        # Now drop the database
        clickhouse_client.execute(f"DROP DATABASE IF EXISTS {database_name}")
        logger.info(f"Database '{database_name}' and all its contents have been successfully dropped.")
        return True

    except Exception as e:
        logger.error(f"Error while dropping the database: {e}")
        return False
# ---------------------------------------------------------
# [Final function] (to call from main)

def prepare_clickhouse() -> bool:
    """
    Perform full ClickHouse setup:
    - load environment variables
    - update configuration
    - install and start container
    - configure user permissions
    - establish client connection
    - reload configuration
    - drop existing database
    - create new database

    Returns:
        bool: True if setup completed successfully, False otherwise.
    """
    try:
        load_dotenv(override=True)
        clickhouse_config = update_ch_config(CH_DB_CONFIG)

        logger.info("Starting ClickHouse preparation")
        if not clickhouse_container_forced_install(clickhouse_config):
            logger.error("ClickHouse container installation failed")
            return False

        if not configure_clickhouse_user_permissions(clickhouse_config):
            logger.error("Setting user permissions failed")
            return False

        client = clickhouse_connection(clickhouse_config)
        if client is None:
            logger.error("Failed to connect to ClickHouse")
            return False

        if not client_reload_clickhouse_config(client, clickhouse_config):
            logger.error("Reloading ClickHouse config failed")
            return False

        if not drop_clickhouse_database(client, clickhouse_config):
            logger.error("Dropping ClickHouse database failed")
            return False

        if not create_database(client, clickhouse_config):
            logger.error("Creating ClickHouse database failed")
            return False

        logger.info("ClickHouse preparation completed successfully")
        return True

    except Exception as e:
        logger.exception(f"Unexpected error in prepare_clickhouse: {e}")
        return False
    
    