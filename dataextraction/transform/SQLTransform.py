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
@Module: Sql Transformer
"""
from  pyspark.ml.feature import SQLTransformer
from transform.Base import Transform
class SQLTransform(Transform):
    """
    @Class: SQL Transform
    @ BaseClass: Transform
    """
    def __init__(self,classflavour):
        self.logger = None
        self.sqlstatement = None
        self.flavour=classflavour

    def init(self, sparkhelper, confighelper, inputdict):
        """
        @Method: init
        @input: Spark helper, confighelper, inputdict
        """
        self.logger = confighelper.getLogger()
        feat_list = self.get_feature_list(inputdict["FeatureList"])
        self.sqlstatement = "SELECT " + feat_list + " FROM __THIS__ "
        if "SQLFilter" in inputdict.keys():
            self.sqlstatement = self.sqlstatement+ "WHERE " + inputdict["SQLFilter"]

        self.logger.debug(" The ML LIB SQL to be executed is " + self.sqlstatement)

    def get_feature_list(self,features_str):
        """
            Wraps all feature argument inside `` character,
            to handle any spaces inside feature names
        """
        q_features = "`_time`,"
        if ( features_str is not None) and len(features_str.strip()) and (features_str.strip() != '*' ):
            features = features_str.split(',')
            for feature in features:
                q_features = q_features + "`" + feature + "`" + ","
            q_features = q_features[:-1]
        else:
            q_features = features_str

        return q_features

    def transform(self, sparksession, sparkdf):
        """
        @Method: transform
        @Inputs sparksession, sparkdf
        """
        sqltrans = SQLTransformer()
        sqltrans.setStatement(self.sqlstatement)
        new_df = sqltrans.transform(sparkdf)
        return new_df
