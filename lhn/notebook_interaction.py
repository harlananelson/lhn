import os
from IPython import get_ipython

from lhn.header import get_logger

logger = get_logger(__name__)

def get_notebook_path():
    """
    Get the path to the current Jupyter notebook.

    Returns:
    - str or None: The notebook path if in a Jupyter environment, otherwise None.
    """
    try:
        shell = get_ipython()
        if 'IPython.notebook' in shell.magics_manager.magics['line']:
            # We are in a Jupyter notebook environment
            notebook_path = shell.ev('IPython.notebook.notebook_path')
            return notebook_path
        else:
            # We are not in a Jupyter notebook environment
            return None
    except NameError:
        # IPython is not defined, so we are not in a Jupyter environment
        return None

# Example usage
# notebook_path = get_notebook_path()
# print(f"Notebook Path: {notebook_path}")

#  Initialize Resources
def setup_environment(notebook_url):
    # 
    import getpass
    from pathlib import Path
    import sys
    systemuser = getpass.getuser()
    notebook_path = os.getcwd()
    python_path, basePath = None, None
    current_directory = os.getcwd()
    project = os.path.basename(current_directory)

    if f"Users/{systemuser}" in notebook_path:
        python_path = os.path.join(Path.home(), 'work', 'Users', systemuser, 'python')
        basePath = os.path.join(Path.home(), 'work', 'Users', systemuser)
    #
    # Check if the notebook is running in the Shared Environment
    elif "lhn-community" in notebook_url and "work/Shared/IUHealth" in notebook_path:
        python_path = os.path.join(Path.home(), 'work', 'Shared', 'IUHealth', 'python')
        basePath = os.path.join(Path.home(), 'work', 'Shared', 'IUHealth')
    
    # Check if the notebook is running in the community IUH environment
    elif "lhn-community" in notebook_url and "work/IUH" in notebook_path:
        python_path = os.path.join(Path.home(), 'work', 'IUH', systemuser, 'python')
        basePath = os.path.join(Path.home(), 'work', 'IUH', systemuser)


    # If neither environment is detected, use a default path
    if python_path is None:
        python_path = os.path.join(Path.home(), 'default', 'path')
        basePath = os.path.join(Path.home(), 'default', 'path')
        if python_path is None:
            python_path = os.path.join(Path.home(), 'default', 'path')
            basePath = os.path.join(Path.home(), 'default', 'path')

    sys.path.append(python_path)

    return current_directory, notebook_path, python_path, basePath, project

