from lhn.header import yaml
from lhn.header import get_logger

logger = get_logger(__name__)

def list2YAML(name, pylist):
    data = {name: pylist}
    yaml_string = yaml.dump(data, default_flow_style=False, sort_keys=False)
    print(yaml_string)
    
    