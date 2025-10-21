from configparser import ConfigParser
import json

# ✅ Database config function
def read_database_config(filename,section):
    """
    Parse the database.ini file and return connection parameters.
    """
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            f"Section {section} not found in the {filename} file."
        )
    return db



# ✅ Optional: Project-level JSON config
def read_project_config(config_file):
    """
    Read a JSON project configuration file and return as dict.
    """
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config