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
    @Module: Factory to create and return instances of Pipeline  
"""
import importlib
# pylint: disable=E0611
from lru import LRU
from ConfigHelper import ConfigHelper
from Pipeline import Pipeline

class FeatureEngineeringFactory():
    """
    @Class: Factory to create and return instances of Pipeline
    """
    def __init__(self, spark_manager):
        """
        @ Constructor
        @ Input: spark_manager
        """
        self.config_help = ConfigHelper()
        self.logger = self.config_help.getLogger()
        self.modulename = ""
        self.default_class = ""
        self.hash_table = LRU(10)
        self.logger.debug("************Starting Init FeatureEngineeringFactory***********")
        try:
            self.spark_session = spark_manager
        except Exception as exc:
            raise Exception('Spark session Error').with_traceback(exc.__traceback__)
    def create_instance(self, baseclass_name, my_classorflavour, inputs_to_class):
        """
        @Function: Makes  Instances
        @ Input  : BaseClass Name & ClassName/FlavourName & Inputs to Class
        @Output  : Class Instance
        """
        self.logger.info("BaseClassName "+ baseclass_name + " Class Flavor "+ my_classorflavour)
        default = self.config_help.isDefault(baseclass_name, my_classorflavour)
        self.modulename = self.config_help.getModuleName(baseclass_name)
        self.default_class = self.config_help.getDefaultClassName(baseclass_name)
        self.logger.debug("Instantiation of module " + self.modulename)
        self.logger.debug("Default Class " + self.default_class)
        self.logger.debug("Class OR Flavour " + my_classorflavour)
        if not default:
            import_class = self.modulename+"."+ my_classorflavour
            self.logger.debug("Class to be imported " + import_class)
            my_class = getattr(importlib.import_module(import_class), my_classorflavour)
            my_classinstance = my_class(my_classorflavour)
        elif default:
            self.logger.debug("Module Name "+self.modulename)
            self.logger.debug("default Class Name "+self.default_class)
            my_class = getattr(importlib.import_module(self.modulename+"."+self.default_class), self.default_class)
            my_classinstance = my_class(my_classorflavour)
        self.logger.debug("Initialization of Class")
        my_classinstance.init(self.spark_session, self.config_help, inputs_to_class)
        return my_classinstance
        
        
    def __makebulk(self, baseclass_name, class_dictionary):
        """
        @Function: Makes Bulk Instances
        @ Input  : BaseClass Name & Dictionay of Classess
        @Output  : List of outputs
        """
        my_instancelist = []
        self.modulename = self.config_help.getModuleName(baseclass_name)
        self.default_class = self.config_help.getDefaultClassName(baseclass_name)
        if baseclass_name != "Transform":
            for my_class in class_dictionary:
                my_instancelist.append(self.create_instance(baseclass_name, my_class, class_dictionary[my_class]))
                self.logger.debug("Created instance for Source/Sink..")
        elif baseclass_name == "Transform":
            for value in class_dictionary:
                my_class = value["operation"]
                inputdict = value
                inputdict.pop("operation")
                self.logger.debug("Instanciating Base Class "+baseclass_name+" My Class "+my_class)
                my_instancelist.append(self.create_instance(baseclass_name, my_class, inputdict))
        return my_instancelist
        
        
    def getBatchPipeline(self, source_classdict, transform_classdict, sink_classdict, caching_key):
        """
        @Function: Makes to get Batch Pipeline
        @ Input  : source Classess, Sink Classess, Transform Classess
        @Output  : Instance of Pipeline Object
        """
        if self.hash_table.get(caching_key) is None:
            self.logger.debug("Cached Instance Not Found, Creating Instance, Key" + caching_key)
            source_instancelist = None
            transformer_instancelist = None
            sink_instancelist = None
            
            source_instancelist = self.__makebulk("Source", source_classdict)
            
            if transform_classdict is not None:
                transformer_instancelist = self.__makebulk("Transform", transform_classdict)
            if sink_classdict is not None:
                sink_instancelist = self.__makebulk("Sink", sink_classdict)
                
            self.hash_table[caching_key] = Pipeline(source_instancelist, transformer_instancelist, sink_instancelist)
        return self.hash_table[caching_key]
