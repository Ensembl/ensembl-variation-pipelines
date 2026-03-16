from typing import Dict, List
import configparser
import subprocess

# Database Client Classes
class DBClient():
    """Base database client"""
    
    def __init__(self, ini_file: str, section: str = "database"):
        self._ini_file = ini_file
        self._section = section

        self._configure_server()

    @property
    def ini_file(self):
        return self._ini_file
    
    @ini_file.setter
    def ini_file(self, ini_file: str) -> str:
        self._ini_file = ini_file
        self._configure_server()

        return self._ini_file
    
    @property
    def section(self):
        return self._section
    
    @section.setter
    def section(self, section: str) -> str:
        self._section = section
        self._configure_server()

        return self._section

    def _configure_server(self) -> None:
        server_config = self._parse_ini()
        
        self.host = server_config.get('host')
        self.port = server_config.get('port')
        self.user = server_config.get('user')
        self.password = server_config.get('password')

    def _parse_ini(self) -> Dict:
        """Parse an ini file and return selected database connection parameters.

        Args:
            ini_file (str): Path to ini file.
            section (str): Section name to read.

        Returns:
            list(dict): Mapping containing 'host', 'port' and 'user'.

        Exits:
            Exits the process if the requested section is not found.
        """
        ini_file = self.ini_file
        section = self.section

        config = configparser.ConfigParser()
        config.read(ini_file)

        server = {}
        if section not in config:
            raise Exception(f"[ERROR] Could not find {section} section in ini file - {ini_file}")
        else:
            server["host"] = config[section]["host"]
            server["port"] = config[section]["port"]
            server["user"] = config[section]["user"]
            if "password" in config[section]:
                server["password"] = config[section]["password"]

        return server
    
    def run_query(self, query: str, dbname: str = None) -> str:
        dbname_part = ""
        if dbname is not None:
            dbname_part = dbname

        process = subprocess.run(
            [
                "mysql",
                "--host", self.host,
                "--port", self.port,
                "--user", self.user,
                dbname_part,
                "-N",
                "--execute",
                query,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if (process.returncode != 0):
            print("[WARNING] Query run failed")
            print(
                f"\tDatabase server - mysql://{self.user}:@{self.host}:{self.port}"
            )
            print(f"\tError - {process.stderr.decode().strip()}")

            return None
        return process.stdout.decode().strip()
    
    # only for test
    def load_sql(self, input_sql: str, dbname: str = None) -> str:
        dbname_part = ""
        if dbname is not None:
            dbname_part = dbname

        with open(input_sql) as file:
            process = subprocess.run(
                [
                    "mysql",
                    "--host", self.host,
                    "--port", self.port,
                    "--user", self.user,
                    dbname_part,
                    "-N"
                ],
                stdin=file,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            print(process)
            if (process.returncode != 0):
                print("[WARNING] SQL loading failed")
                print(
                    f"\tDatabase server - mysql://{self.user}:@{self.host}:{self.port}"
                )
                print(f"\tError - {process.stderr.decode().strip()}")

                return None

            return process.stdout.decode().strip()
        
        return None
    
# gRPC Client Classes
class GRPCClient():
    """Base database client"""
    pass