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
@Module: Spark Default Source
"""
from source.Base import Source

class DefaultSparkSource(Source):
    """
    @Class: Spark Default Source
    """
    def __init__(self,classflavour):
        """
        @Method: Single Arg Constructor
        """
        self.class_type="Default"
        self.flavour=classflavour
        self.logger=None
        self.sparkloadkey=None
    def init(self,sparkhelper,confighelper,inputdict):
        """
        @Method:Init
        @Inputs: sparkhelper, confighelper , inputdict
        @Output: none
        """
        self.logger = confighelper.getLogger()
        classconfig,sparkconfig  = confighelper.getClassConfig(self)
        self.logger.debug("Set Class Spark Source Flavor"+self.flavour)
        self.sparkloadkey=classconfig["SparkLoadKey"]
        for key in sparkconfig:
            value = sparkconfig[key]
            if value.startswith('$Input$'):
                inputkey = value[7:]
                sparkhelper.add_conf(key,inputdict[inputkey])
            else:
                sparkhelper.add_conf(key,value)
        self.logger.debug("Spark Default Source Initialized as"+self.flavour)
    def load(self,sparksession):
        """
        @Method: load
        @Inputs: sparksession
        @Output: dataframe
        """
        df_data = sparksession.read.format(self.sparkloadkey).load()
        self.logger.debug("Data Loaded from Source"+ self.flavour)
        return df_data
