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
@ModuleName: Sink Default Class
"""
from sink.Base import Sink
class DefaultSparkSink(Sink):
    """
    @Class: Default Spark Sink
    """
    def __init__(self, classflavour):
        """
        @Methond: Constructor
        @Input : classflavor
        """
        self.ClassType="Default"
        self.flavour = classflavour
        self.logger = None
        classconfig = None
        sparkconfig  = None
        self.sparkloadkey = None
        self.writetype = None
    def init(self, sparkhelper, confighelper, inputdict):
        """
        @Methond:init
        @Inputs: sparkhelper
                 confighelper
                 inputdict
        """
        self.logger= confighelper.getLogger()
        classconfig,sparkconfig  = confighelper.getClassConfig(self)
        self.sparkloadkey=classconfig["SparkLoadKey"]
        self.writetype=classconfig["WriteMode"]
        for key in sparkconfig:
            value = sparkconfig[key]
            print("Spark Config",key,sparkconfig[key])
            if value.startswith('$Input$'):
                inputkey = value[7:]
                sparkhelper.addConf(key,inputdict[inputkey])
            else:
                sparkhelper.addConf(key,value)
    def write(self, sparksession, sparkdf):
        """
        @Method: write
        @input : sparksession
                 sparkdf
        """
        sparkdf.write.format(self.sparkloadkey).mode(self.writetype).save()
        self.logger.debug("Data written to Sink"+self.flavour)
