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
@Module : Base
"""
from abc import abstractmethod

class Transform():
    """
    @Class: Transform class
    """
    def __init__(self):
        """
        @Method: No Args Constructor
        """
        self.class_type="Custom"
    @abstractmethod
    def init(self, sparkhelper, confighelper, inputdict):
        """
        @Methond: init
        """
    @abstractmethod
    def transform(self,sparksession, sparkdf):
        """
        @Methond: transform
        @Inpuuts: spark
        """
        
