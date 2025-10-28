#!/bin/bash
# DQ Post Deployment Script with Vulnerable JAR Handling and Logging

set -euo pipefail
IFS=$'\n\t'

# --- CONFIGURATION ---
rdata=20161024
version=${1:-${rdata}.0.BUILD-RELEASE}
user=${2:-cdhqa}
src_path=/tmp/release_${rdata}
target_path=/opt/${user}/release/services
deploy_folder=r${rdata}
file_ext=Dir.tar.gz
link_path=/opt/${user}/apps/services/
vuln_dir="/opt/${user}/secure_libs"
log_file="/opt/${user}/deployment_${rdata}.log"

# --- LOGGING FUNCTION ---
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] $2" | tee -a "${log_file}"
}

mkdir -p "${link_path}" "${vuln_dir}"

declare -a array0=(registrationserver configurationservice hiveperiphservice IngestionLauncher jdbcingestionserver)
declare -a array1=(DataServicesRegistrationServer DataServicesConfigurationServer DataServicesHivePeripheralServer DataServicesIngestionLauncher DataServicesJdbcIngestionServer)

log INFO "Starting post-deployment for version ${version}"

# --- MAIN LOOP ---
for ((i=0; i<${#array0[@]}; i++))
do
    svc_name=${array0[$i]}
    svc_artifact=${array1[$i]}
    tgt_path=${target_path}/${svc_name}/${deploy_folder}

    log INFO "------------------------------------------------------------"
    log INFO "Deploying service: ${svc_name}"
    log INFO "Target path: ${tgt_path}"

    # Backup existing target
    if [ -d "${tgt_path}" ]; then
        log INFO "Existing deployment found. Backing up to ${tgt_path}_prev"
        mv "${tgt_path}" "${tgt_path}_prev"
    fi

    mkdir -p "${tgt_path}"
    cp "${src_path}/${svc_artifact}-${version}-${file_ext}" "${tgt_path}/"
    cd "${tgt_path}"

    log INFO "Extracting ${svc_artifact}-${version}-${file_ext}"
    tar -xvzf "${svc_artifact}-${version}-${file_ext}" | tee -a "${log_file}"

    # --- Vulnerable JAR Management ---
    lib_dir="${tgt_path}/3rdlib"
    if [ -d "${lib_dir}" ]; then
        log INFO "Scanning ${lib_dir} for vulnerable JARs..."
        cd "${lib_dir}"

        for jar_pattern in "spring-core*.jar" "spring-webmvc*.jar" "kafka-clients*.jar"
        do
            for jar_file in ${jar_pattern}; do
                if [ -f "${jar_file}" ]; then
                    base_name=$(basename "${jar_file}")

                    # Move only if not already centralized
                    if [ ! -f "${vuln_dir}/${base_name}" ]; then
                        log INFO "Moving ${jar_file} to ${vuln_dir}/"
                        mv "${jar_file}" "${vuln_dir}/"
                    else
                        log INFO "File ${base_name} already exists in ${vuln_dir}, skipping move."
                        rm -f "${jar_file}"
                    fi

                    # Create symbolic link inside 3rdlib
                    ln -sfn "${vuln_dir}/${base_name}" "${lib_dir}/${base_name}"
                    log INFO "Created symlink: ${lib_dir}/${base_name} → ${vuln_dir}/${base_name}"
                fi
            done
        done
    else
        log WARN "No 3rdlib directory found for ${svc_name}"
    fi

    # --- Update application-level symlink ---
    cd "${link_path}"
    if [ -f "${svc_name}" ] || [ -L "${svc_name}" ]; then
        log INFO "Backing up existing link for ${svc_name}"
        rm -f "${svc_name}_prev"
        mv "${svc_name}" "${svc_name}_prev"
    fi
    ln -sfn "${tgt_path}" "${svc_name}"
    log INFO "Updated symlink: ${svc_name} → ${tgt_path}"
done

log INFO "------------------------------------------------------------"
log INFO "Deployment completed successfully for all services."
log INFO "Logs saved to ${log_file}"
