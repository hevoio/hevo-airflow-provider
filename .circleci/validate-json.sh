#!/bin/bash
# set -x
UNSET_FIELDS=()
new_repo_details_file=$(git diff-tree --no-commit-id --name-only ${CIRCLE_SHA1} -r)
# new_repo_details_file=repos/new-repo-details-6.json
repo_name=$(cat ${new_repo_details_file} | jq -r '.|.["repo-name"]')
repo_full_name=hevoio/${repo_name}
repo_description=$(cat ${new_repo_details_file} | jq -r '.|.["repo-description"]')
team=$(cat ${new_repo_details_file} | jq -r '.team')
jira_id=$(cat ${new_repo_details_file} | jq -r '.|.["jira-id"]')
confluence_url=$(cat ${new_repo_details_file} | jq -r '.|.["confluence-url"]')
code_owners=$(cat ${new_repo_details_file} | jq -r '.|.["code-owners"]')
repo_language=$(cat ${new_repo_details_file} | jq -r '.|.["repo-config"] .language')
echo "export new_repo_details_file=${new_repo_details_file}" >> $BASH_ENV
echo "export repo_full_name=${repo_full_name}" >> $BASH_ENV
echo "export new_repo_details_file=${new_repo_details_file}" >> $BASH_ENV
echo "export repo_name=${repo_name}" >> $BASH_ENV
echo "export jira_id=${jira_id}" >> $BASH_ENV
echo "export confluence_url=${confluence_url}" >> $BASH_ENV
echo "export repo_language=${repo_language}" >> $BASH_ENV
echo "export code_owners=\"${code_owners}\"" >> $BASH_ENV
echo "export repo_description=\"${repo_description}\"" >> $BASH_ENV
if [[ "java" == ${repo_language} ]]; then
    group_id=$(cat ${new_repo_details_file} | jq -r '.|.["repo-config"] .["group-id"]')
    echo "export group_id=${group_id}" >> $BASH_ENV
    artefact_id=$(cat ${new_repo_details_file} | jq -r '.|.["repo-config"] .["artefact-id"]')
    echo "export artefact_id=${artefact_id}" >> $BASH_ENV
    repo_type=$(cat ${new_repo_details_file} | jq -r '.|.["repo-config"] .["type"]')
    echo "export repo_type=${repo_type}" >> $BASH_ENV
    if [[ -z "$group_id" || -z "${!var}" ]]; then
        UNSET_FIELDS+=("group-id")
    fi
    if [[ $artefact_id == null || -z "${!var}" ]]; then
        UNSET_FIELDS+=("artefact-id")
    fi
    if [[ $repo_type == null || -z "${!var}" ]]; then
        UNSET_FIELDS+=("type")
    fi
fi

while read var; do
    if [[ "${!var}" == null || -z "${!var}" ]]; then
        UNSET_FIELDS+=("$var")
    fi
done << EOF
repo_name
repo_description
team
jira_id
confluence_url
code_owners
repo_language
EOF

echo "${UNSET_FIELDS[@]}"