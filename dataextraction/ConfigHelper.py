# ==================================================================================
#
#       Copyright (c) 2022 Samsung Electronics Co., Ltd. All Rights Reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# ==================================================================================

"""
@module: Config Helper
"""
import configparser
import collections
import os
import re
from tmgr_logger import TMLogger
from exceptions_util import DataExtractionException


class Singleton(type):
    """
    Class : Singleton
    """
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class ConfigHelper(metaclass=Singleton):
    """
    Class: Confighelper Class
    """
    def __init__(self):
        """
        Function : No Args Constructor for Confighelper class
        """
        self.moduledetails = collections.defaultdict(dict)
        self.envconfig = collections.defaultdict(dict)
        try:

            self.exec_path = os.getenv('CODE_DIR_PATH')
            
            if self.exec_path is None:
                self.tm_logger = TMLogger("../config/log_config.yaml")
            else:
                self.tm_logger = TMLogger(self.exec_path + "/log_config.yaml")


            self.logger = self.tm_logger.logger
            self.log_level = self.tm_logger.log_level
            self.logger.debug("+++++++++++++++Initializaing Config Helper +++++++++++++++++++++++")
            self.logger.debug(str(self.exec_path))
            self.fs_dict ={}
            self.fs_dict['fs_host'] = os.getenv('FS_API_HOST')
            self.fs_dict['fs_port'] = os.getenv('FS_API_PORT')
            self.configsource = configparser.ConfigParser()       
            self.configtransform = configparser.ConfigParser()
            self.configsink = configparser.ConfigParser()
            if self.exec_path is None:
                self.configsource.read("./source/SourceClassConfig.ini",encoding = "utf-8")
                self.configtransform.read("./transform/TransformClassConfig.ini",encoding = "utf-8")
                self.configsink.read("./sink/SinkClassConfig.ini",encoding = "utf-8")
            else:
                self.configsource.read(self.exec_path+"/source/SourceClassConfig.ini",encoding = "utf-8")
                self.configtransform.read(self.exec_path+"/transform/TransformClassConfig.ini",encoding = "utf-8")
                self.configsink.read(self.exec_path+"/sink/SinkClassConfig.ini",encoding = "utf-8")
            self.__setmoduledetails("Source")
            self.__setmoduledetails("Sink")
            self.__setmoduledetails("Transform")
            self.__setenvconfig("Source")
            self.__setenvconfig("Sink")
            self.__setenvconfig("Transform")
        except Exception as exp:
            raise Exception('Error in environment Variables').with_traceback(exp.__traceback__)
        self.logger.debug("completed Initialization of ConfigHelper")

    def __setenvconfig(self,baseclassname):
        """
        Function: Private: Get Environment Configuration
        """
        config = self.getConfigSource(baseclassname)
        try:
            self.envconfig_file = config["EnvConfig"]
            for key in self.envconfig_file:
                self.logger.debug("Getting Env Variables for: " + baseclassname)
                value=os.getenv(self.envconfig_file[key])
                if value is None:
                    raise DataExtractionException("Error Environment Variable Not Set"+self.envconfig_file[key] )
                self.envconfig[key]=value
                self.logger.debug("Read Environment Config var: "+self.envconfig_file[key])
        except Exception as exc:
            raise Exception('Reading environment Variables').with_traceback(exc.__traceback__)
    def __setmoduledetails(self,baseclassname):
        """
        Function: Private Function : setModule details
        Input: base class name
        """
        try:
            config = self.getConfigSource(baseclassname)
            module = config["ModuleDetails"]
            self.moduledetails[baseclassname]["ModuleName"] = module["ModuleName"]
            self.moduledetails[baseclassname]["DefaultClassName"] = module["DefaultClassName"]
        except Exception as exp:
            raise Exception('Reading Module Detail for baseclass').with_traceback(exp.__traceback__)
    def getClassConfig(self,my_classinstance):
        """
        Function: GetClassConfiguration for the specified class instance
        Input: Class Instance
        
        """
        try:
            classflavour = "Error"
            if my_classinstance.class_type == "Default":
                classflavour = my_classinstance.flavour
            elif my_classinstance.class_type == "Custom":
                classflavour = my_classinstance.__class__.__name__
            baseclass=my_classinstance.__class__.__base__.__name__
            self.logger.debug("baseclass is Set to "+baseclass)
            configsource = self.getConfigSource(baseclass)
            self.logger.debug("ClassFlavour "+ classflavour)
            source_props = configsource[classflavour]
            self.logger.debug("Source basic properties:" + str(source_props))
            
            source_props_dict = self.__InjectEnvValues(source_props)
            if ((source_props_dict["class_type"] == "Default" or
                    source_props_dict["class_type"] == "Custom" ) 
                    and baseclass != "Transform" and classflavour != 'InfluxSource'):
                injectedconfigsparkconf= self.\
                __InjectEnvValues(configsource[classflavour+".SparkConf"])
                return source_props_dict, injectedconfigsparkconf

            return source_props_dict
        except  Exception as exp:
            self.logger.debug(exp.__traceback__)
            raise Exception('Error in getting Class Config '+classflavour).\
            with_traceback(exp.__traceback__)

    def isDefault(self,baseclassname,my_classname):
        """
        Function: isDefault
        Input: baseclassname, my_classname
        Output: True if default False otherwise
        """
        try:
            config = self.getConfigSource(baseclassname)
            test= config[my_classname]
            if test["class_type"] == "Default":
                return True
            return False
        except Exception as exp:
            self.logger.error("Error in Class Type "+ str(exp))
            raise Exception('Config Class Type for '+baseclassname).\
            with_traceback(exp.__traceback__)
    def getDefaultClassName(self,baseclassname):
        """
        Function:getDefaultClassName
        Input: baseclassname
        """
        try:
            return self.moduledetails[baseclassname]["DefaultClassName"]
        except Exception as exp:
            raise Exception('Module Details miss configured '+baseclassname).\
            with_traceback(exp.__traceback__)
    def getModuleName(self,baseclassname):
        """
        Function:Get Model Name
        Inputs: base class name
        """
        try:
            return self.moduledetails[baseclassname]["ModuleName"]
        except Exception as exp:
            raise Exception('Module Details miss configured '+baseclassname).\
            with_traceback(exp.__traceback__)
    def getConfigSource(self,baseclass):
        """
        Function:Get Config Source
        Inputs base class
        Return: Configuration for the base class
        """
        if baseclass == "Source":
            return self.configsource
        elif baseclass == "Transform":
            return self.configtransform
        else:
            return self.configsink
    def __InjectEnvValues(self,configdict):
        """
        Function : Inject Env Values, from default_config
                    reads, populate only those conf, which has prefix $ENV
                    
        """
        try:
            for key in configdict:
                string = configdict[key]
                matchlist=re.findall(r'\$ENV\{\w*\}',string) # \w stands for [a-zA-Z_0-9]
                self.logger.debug("Injecting Env Values for,key-value " + key + " " + string )
                for replacestr in matchlist:
                    envkey= replacestr[5:-1]
                    envkey =envkey.lower()
                    replacevalue = self.envconfig[envkey]
                    if replacevalue is None or not isinstance(replacevalue,str):
                        raise DataExtractionException("environment variable Not found"\
                                        +self.envconfig_file[envkey])
                    string = string.replace(replacestr,replacevalue)
                configdict[key] = string
            return configdict
        except Exception as exp:
            raise Exception('Error in Environment Variables Configuration').\
            with_traceback(exp.__traceback__)

    def getEnvConfig(self):
        """
        Function: Get all the environment variable as dictionery 

        """
        return self.envconfig

    def getLogLevel(self):
        """
        Function: Get Log Level
        """
        return self.log_level
    def getLogger(self):
        """
        Function : Get Logger
        """
        return self.logger
    def getFsHost(self):
        """
        Function: Get FS Host
        """
        return self.fs_dict['fs_host']
    def getFsPort(self):
        """
        Function: Get FS Port
        """
        return self.fs_dict['fs_port']
