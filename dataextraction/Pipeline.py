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
@Module : Feature Engineering Pipeline
"""
from ConfigHelper import ConfigHelper

class Pipeline():
    """
    @Class: Feature Engineering Pipeline
    """
    def __init__(self, sources, transformers, sinks):
        """
        @Constructor
        """
        self.logger = ConfigHelper().logger
        self.sources = sources
        self.transformers = transformers
        self.sinks = sinks
        self.logger.debug("Pipeline Created")
        self.spark_dflist = None
        self.transformed_df = None
    def loadData(self, session):
        """
        @Function: Loads data from source
        """
        self.logger.info("Source:" + str(self.sources[0]))
        self.spark_dflist = self.sources[0].load(session)
        self.logger.info("Data Load Completed")
    def transformData(self, session):
        """
        @Function : Transform Data
        """
        if self.transformers is None:
            self.transformed_df = self.spark_dflist
        else:
            self.transformed_df = self.transformers[0].transform(session, self.spark_dflist)
        self.logger.info("Data Transform Completed")
    def writeData(self, session):
        """
        @Function: Write Data
        """
        self.sinks[0].write(session, self.transformed_df)
        self.logger.info("Data Written to Sink")
    def execute(self, session):
        """
        @Function : Execute Pipeline
        """
        self.loadData(session)
        self.transformData(session)
        self.writeData(session)
