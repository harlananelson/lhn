
from lhn.header import inspect, OrderedDict, pprint
from lhn.header import get_logger

logger = get_logger(__name__)

def missingParameters(attributesParam, function):
    """
    Return the items in common with the parameter arguments in function and the items of the dictionary attributesParam
    """
    
    args                = get_default_args(function)
    argsEnum            = enumerate(inspect.getfullargspec(function).args)
    missingAssignments  = {item for i,item  in argsEnum if item not in attributesParam.keys() and item not in args.keys()}
    
    return(missingAssignments)

def get_parameters(attributes_param, function, config_dict, debug=False):
    """
    Return the parameters of 'function' including default parameters in the order listed in the function definition.
    The parameters are combined with the items in common with the parameter arguments in 'function' and the items
    of the dictionary 'attributes_param'.

    :param attributes_param: Dictionary of attributes to update the function parameters.
    :param function: The function whose parameters are to be retrieved.
    :param config_dict: Base configuration dictionary to which 'attributes_param' will be added.
    :param debug: Flag to enable debug print statements.
    :return: An OrderedDict of the parameters of the function.
    """
    # Create a local copy of the config dictionary and update it with attributes_param
    config_dict_local = config_dict.copy()
    config_dict_local.update(attributes_param)
                        
    # Get the default key:value pairs for arguments because those don't need to be supplied
    args_default = get_default_args(function)
    if debug:
        print(f"args_default: {args_default}")
    
    # Get a list of all arguments in the order they are defined in the function
    fn_args = inspect.getfullargspec(function).args
    if debug:
        print(f"fn_args: {fn_args}")
    
    # Filter the updated dictionary to only include keys that match the function's arguments
    new_values = {key: value for key, value in config_dict_local.items() if key in fn_args}
    if debug:
        print(f"new_values: {new_values}")
        
    # Create an ordered dictionary with the function's arguments and update with default and new values
    result = OrderedDict.fromkeys(fn_args, 0)
    result.update(args_default)
    result.update(new_values)
        
    return result

def set_function_parameters(function, fun_param={}, config_dict={}, update=False, debug=False):
    if update:
        config_dict.update(fun_param)

    config_dict_local = config_dict.copy()
    config_dict_local.update(fun_param)
    if debug:
        print(f"Missing missing_parameters({config_dict_local, function})")
    add_call = get_parameters(config_dict_local, function, config_dict={}, debug=debug)
    add_call.update(fun_param)
    if debug:
        pprint.pprint({key: value for key, value in add_call.items() if key not in ['DF']})
    return add_call

def getParameters(attributesParam, function, config_dictt, debug = False):
    """
    Return the items in common with the parameter arguments in function and the items of the dictionary attributesParam
    Return a list of the parameters of the function including default parameters in the order listed in the function definition
    """
    config_dicttLocal = config_dictt.copy()
    config_dicttLocal.update(attributesParam)
                        
    # The default key:values for arguments because those don't need to be supplied
    argsDefault         = get_default_args(function)
    if debug: print(f"argsDefault: {argsDefault}")
    
    # Need a list of all arguments in the order they are defined in the function
    fnArgs              = inspect.getfullargspec(function).args
    if debug: print(f"funArgs: {fnArgs}")
    
    newValues = {key:value for key,value in config_dicttLocal.items() if key in fnArgs}
    if debug: print(f"newValues: {newValues}")
        
    result = OrderedDict.fromkeys(fnArgs, 0)
    
    result.update(argsDefault)
    result.update(newValues)
        
        
    return(result)

def get_parameters(attributes_param, function, config_dict, debug=False):
    """
    Return the parameters of 'function' including default parameters in the order listed in the function definition.
    The parameters are combined with the items in common with the parameter arguments in 'function' and the items
    of the dictionary 'attributes_param'.

    :param attributes_param: Dictionary of attributes to update the function parameters.
    :param function: The function whose parameters are to be retrieved.
    :param config_dict: Base configuration dictionary to which 'attributes_param' will be added.
    :param debug: Flag to enable debug print statements.
    :return: An OrderedDict of the parameters of the function.
    """
    # Create a local copy of the config dictionary and update it with attributes_param
    config_dict_local = config_dict.copy()
    config_dict_local.update(attributes_param)
                        
    # Get the default key:value pairs for arguments because those don't need to be supplied
    args_default = get_default_args(function)
    if debug:
        print(f"args_default: {args_default}")
    
    # Get a list of all arguments in the order they are defined in the function
    fn_args = inspect.getfullargspec(function).args
    if debug:
        print(f"fn_args: {fn_args}")
    
    # Filter the updated dictionary to only include keys that match the function's arguments
    new_values = {key: value for key, value in config_dict_local.items() if key in fn_args}
    if debug:
        print(f"new_values: {new_values}")
        
    # Create an ordered dictionary with the function's arguments and update with default and new values
    result = OrderedDict.fromkeys(fn_args, 0)
    result.update(args_default)
    result.update(new_values)
        
    return result


def setFunctionParameters(function, funParam={}, config_dict={}, update=False, debug=False):
    
    if update:
        config_dict.update(funParam)
        locals().update(config_dict)
        
    config_dict2 = config_dict.copy()
    config_dict2.update(funParam)

    if debug:
        print(f"Missing {missingParameters(config_dict2, function)}")

    addCall = getParameters(config_dict2, function, config_dictt={}, debug=debug)
    addCall.update(funParam)

    # Exclude 'self' from the dictionary
    filtered_addCall = {key: value for key, value in addCall.items() if key != 'self'}

    if debug:
        pprint.pprint({key: value for key, value in filtered_addCall.items() if key not in ['DF']})

    return filtered_addCall


def get_default_args(func):
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }
