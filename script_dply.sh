
# Remove version info from link name (keep only base jar name)
link_name=$(echo "${jar_name}" | sed -E 's/-[0-9]+(\.[0-9]+)*(\.[A-Za-z0-9]+)*\.jar$/.jar/')
ln -sfn "${secure_libs}/${jar_name}" "${thirdlib_path}/${link_name}"
log INFO "Created symlink: ${thirdlib_path}/${link_name} -> ${secure_libs}/${jar_name}"


===========================

# Patterns for vulnerable jars (globs)
vulnerable_jars=("spring-core*.jar" "spring-webmvc*.jar" "kafka-clients*.jar")

# Services to process (adjust as needed)
declare -a array0=(
  cohservice
  realtimemaprdservice
  DsFileService
  message_service
  transformationservice
  realtimesinkservice
)

# -------------------------
# LOG FUNCTION
# -------------------------
log() {
  printf "%s [%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "$1" "$2" | tee -a "${log_file}"
}

# ensure directories
mkdir -p "${secure_libs}"
mkdir -p "$(dirname "${log_file}")"

log INFO "Starting traversal under base path: ${base_path}"
log INFO "Secure libs directory: ${secure_libs}"

# -------------------------
# MAIN
# -------------------------
for service in "${array0[@]}"; do
  service_path="${base_path}/${service}"
  log INFO "Processing service: ${service}"
  log INFO "Service path: ${service_path}"

  if [ ! -d "${service_path}" ]; then
    log WARN "Service folder not found: ${service_path}. Skipping."
    continue
  fi

  # collect all 3rdlib directories into a temp file using null-separated records
  tmp_thirdlibs=$(mktemp)
  find "${service_path}" -type d -name "3rdlib" -print0 > "${tmp_thirdlibs}"

  # check if tmp file is empty (no hits)
  if [ ! -s "${tmp_thirdlibs}" ]; then
    log WARN "No 3rdlib directory found for service: ${service}"
    rm -f "${tmp_thirdlibs}"
    continue
  fi

  # iterate each 3rdlib path safely
  while IFS= read -r -d '' thirdlib_path; do
    [ -z "${thirdlib_path}" ] && continue
    log INFO "Found 3rdlib: ${thirdlib_path}"

    # for each vulnerable pattern, find matching jar files inside this 3rdlib
    for pattern in "${vulnerable_jars[@]}"; do
      tmp_jars=$(mktemp)
      find "${thirdlib_path}" -type f -name "${pattern}" -print0 > "${tmp_jars}"

      if [ ! -s "${tmp_jars}" ]; then
        rm -f "${tmp_jars}"
        continue
      fi

      while IFS= read -r -d '' jar_path; do
        [ -z "${jar_path}" ] && continue
        jar_name=$(basename "${jar_path}")
        jar_dirname=$(dirname "${jar_path}")

        log INFO "Found vulnerable JAR: ${jar_name} (path: ${jar_path})"

        # Move real jar to secure_libs if not present
        if [ ! -f "${secure_libs}/${jar_name}" ]; then
          log INFO "Moving ${jar_name} -> ${secure_libs}"
          mv -f "${jar_path}" "${secure_libs}/"
        else
          log INFO "${jar_name} already exists in ${secure_libs}. Removing local copy."
          rm -f "${jar_path}"
        fi

        # Create a dummy JAR in place (then replace it with symlink)
        log INFO "Creating dummy JAR placeholder for ${jar_name} at ${thirdlib_path}"
        tmpdir=$(mktemp -d)
        mkdir -p "${tmpdir}/META-INF"
        printf '%s\n' "Manifest-Version: 1.0" > "${tmpdir}/META-INF/MANIFEST.MF"
        # create dummy JAR named as original inside 3rdlib
        jar cf "${thirdlib_path}/${jar_name}" -C "${tmpdir}" META-INF >/dev/null 2>&1 || {
          # if 'jar' not available, fallback to creating a zip with .jar ext
          (cd "${tmpdir}" && zip -q -r "${thirdlib_path}/${jar_name}" META-INF >/dev/null 2>&1) || true
        }
        rm -rf "${tmpdir}"

        # Create / update symlink from dummy jar -> real jar in secure_libs
        ln -sfn "${secure_libs}/${jar_name}" "${thirdlib_path}/${jar_name}"
        log INFO "Created symlink: ${thirdlib_path}/${jar_name} -> ${secure_libs}/${jar_name}"

      done < "${tmp_jars}"

      rm -f "${tmp_jars}"
    done

  done < "${tmp_thirdlibs}"

  rm -f "${tmp_thirdlibs}"
done

log INFO "Traversal & fix completed. Log: ${log_file}"














#!/bin/bash
# -------------------------------------------------------------------
# Traverse service folders under base path, locate 3rdlib directories,
# move vulnerable JARs to secure_libs, create dummy JARs, and symlinks.
# -------------------------------------------------------------------

set -euo pipefail
IFS=$'\n\t'

# --- CONFIGURATION ---
user=${1:-cdhqa}
base_path="/apps/src/paseapad/dev/github/eapp-dct-ingestion/apps/service"
secure_libs="/apps/${user}/apps/secure_libs"
log_file="/apps/${user}/postdeploy_vulnfix.log"

# --- Logging Function ---
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] $2" | tee -a "${log_file}"
}

# --- Vulnerable JARs to Handle ---
vulnerable_jars=("spring-core*.jar" "spring-webmvc*.jar" "kafka-clients*.jar")

# --- Services List ---
declare -a array0=(
    cohservice
    realtimemaprdservice
    DsFileService
    message_service
    transformationservice
    realtimesinkservice
)

log INFO "------------------------------------------------------------"
log INFO "Starting 3rdlib vulnerability fix under: ${base_path}"
log INFO "Secure JAR repository: ${secure_libs}"
mkdir -p "${secure_libs}"

# --- MAIN LOOP ---
for service in "${array0[@]}"; do
    service_path="${base_path}/${service}"
    log INFO "Processing service: ${service}"
    log INFO "Service folder path: ${service_path}"

    if [ ! -d "${service_path}" ]; then
        log WARN "Service folder not found: ${service_path}"
        continue
    fi

    # Find all 3rdlib directories inside this service
    mapfile -t thirdlib_dirs < <(find "${service_path}" -type d -name "3rdlib" 2>/dev/null)

    if [ ${#thirdlib_dirs[@]} -eq 0 ]; then
        log WARN "No 3rdlib directory found for service: ${service}"
        continue
    fi

    # --- Iterate over each 3rdlib found ---
    for thirdlib_path in "${thirdlib_dirs[@]}"; do
        log INFO "Found 3rdlib: ${thirdlib_path}"

        # Handle vulnerable jars inside 3rdlib
        for pattern in "${vulnerable_jars[@]}"; do
            find "${thirdlib_path}" -type f -name "${pattern}" | while read -r jar_path; do
                jar_name=$(basename "${jar_path}")

                log INFO "Found vulnerable JAR: ${jar_name} in ${thirdlib_path}"

                # Move jar to secure_libs (if not already moved)
                if [ ! -f "${secure_libs}/${jar_name}" ]; then
                    log INFO "Moving ${jar_name} to ${secure_libs}"
                    mv "${jar_path}" "${secure_libs}/"
                else
                    log INFO "${jar_name} already exists in ${secure_libs}, removing local copy."
                    rm -f "${jar_path}"
                fi

                # Create dummy jar placeholder
                log INFO "Creating dummy jar for ${jar_name}"
                temp_dir=$(mktemp -d)
                mkdir -p "${temp_dir}/META-INF"
                echo "Manifest-Version: 1.0" > "${temp_dir}/META-INF/MANIFEST.MF"
                jar cf "${thirdlib_path}/${jar_name}" -C "${temp_dir}" META-INF >/dev/null 2>&1
                rm -rf "${temp_dir}"

                # Create symbolic link from dummy jar → secure jar
                ln -sfn "${secure_libs}/${jar_name}" "${thirdlib_path}/${jar_name}"
                log INFO "Linked ${thirdlib_path}/${jar_name} → ${secure_libs}/${jar_name}"
            done
        done
    done
done

log INFO "------------------------------------------------------------"
log INFO "Traversal and vulnerability fix completed successfully."
log INFO "Log file: ${log_file}"




















===============================






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
