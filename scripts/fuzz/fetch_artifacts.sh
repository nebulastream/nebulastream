#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ -z "$GITHUB_API_TOKEN" ]
then
    echo pls set GITHUB_API_TOKEN
    exit 1
fi

if [ ! -d ma_results ]
then
    echo expexts ./ma_results to exits
    exit 1
fi

res=./ma_results

curl -L \
    --silent \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $GITHUB_API_TOKEN" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    https://api.github.com/repos/nebulastream/nebulastream/actions/artifacts \
    > $res/artifacts.json

downloaded=0
for artifact_id in $(jq < $res/artifacts.json -r '.artifacts.[] | select(.workflow_run.head_branch == "fuzz") | .id')
do
    if [ -f $res/$artifact_id.zip ] && unzip -t $res/$artifact_id.zip > /dev/null
    then
        echo $artifact_id.zip already exists
        continue
    fi

    curl -L \
      --silent \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer $GITHUB_API_TOKEN" \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      --output $res/$artifact_id.zip \
      https://api.github.com/repos/nebulastream/nebulastream/actions/artifacts/$artifact_id/zip

    downloaded=$(( downloaded + 1))
done

echo dowloaded $downloaded artifacts to $res
