# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

dataform install

echo "{\"projectId\": \"${PROJECT_ID}\", \"location\": \"${BQ_LOCATION}\"}" >> .df-credentials.json

# Need to specify a separate flag for each individual action/tag/var value ie. dataform run --tags example1 --tags example2 --tags example3
#  https://github.com/dataform-co/dataform/issues/1200
# The bash line below replaces all occurrences of comma
# in $DATAFORM_TAGS with the string '--tags'
# Bash parameter expansion is used to do this:
#   ${parameter//find/replace}
# https://www.gnu.org/software/bash/manual/html_node/Shell-Parameter-Expansion.html
if [[ -n $DATAFORM_TAGS ]]; then
  all_dataform_tags="--tags ${DATAFORM_TAGS//,/ --tags }"
fi

if [[ -n $DATAFORM_ACTIONS ]]; then
  all_dataform_actions="--actions ${DATAFORM_ACTIONS//,/ --tags }"
fi

printf """
Executing the following dataform command:
************************************************************
dataform run %s %s --vars=src_database=${src_database},src_schema=${src_schema},src_table=${src_table},target_database=${target_database},target_schema=${target_schema},target_table=${target_table},target_hash_unique_col_name=${target_hash_unique_col_name},target_hash_non_unique_col_name=${target_hash_non_unique_col_name},timestampfield=${timestampfield},start_from_column_name=${start_from_column_name},end_at_column_name=${end_at_column_name},from_date=${from_date},to_date=${to_date},portion_size=${portion_size}
************************************************************
""" "${all_dataform_tags}" "${all_dataform_actions}"

#retries=3 #set max numnber of retries

#for ((i=0; i<retries; i++)); do
#    dataform run --run-tests $(echo "${all_dataform_tags}" "${all_dataform_actions}" | xargs)
#    [[ $? -eq 0 ]] && break

#    echo "step failed, let's wait 60 seconds and retry"
#    sleep 60 #time to wait before trying again
#done

#(( retries == i )) && { echo 'Failed!'; exit 1; }
#exit 0

dataform run $(echo "${all_dataform_tags}" "${all_dataform_actions}" | xargs) --vars=src_database=${src_database},src_schema=${src_schema},src_table=${src_table},target_database=${target_database},target_schema=${target_schema},target_table=${target_table},target_hash_unique_col_name=${target_hash_unique_col_name},target_hash_non_unique_col_name=${target_hash_non_unique_col_name},timestampfield=${timestampfield},start_from_column_name=${start_from_column_name},end_at_column_name=${end_at_column_name},from_date=${from_date},to_date=${to_date},portion_size=${portion_size}
