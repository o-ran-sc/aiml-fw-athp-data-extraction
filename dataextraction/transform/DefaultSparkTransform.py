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
@Module : Currently UI Hardcoded to use SQL  transformer, to be implemented if required
"""
from transform.Base import Transform
class DefaultSparkTransform(Transform):
    """
    @Module : To be implemented
    """
    def __init__(self,classflavour):
        """
        @Method:constructor
        """
        self.class_type="Default"
        self.flavour=classflavour
    def init(self,sparkhelper, confighelper,inputdict):
        """
        @Methond: init to be implemented
        """
        pass

    def transform(self,sparksession,sparkdf):
        """
        @Method:Generic transform to be implemented
        """
        pass
