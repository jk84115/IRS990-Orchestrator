#!/usr/bin/env python3
# main.py - Orchestrator for the Investigative Journalism Workflow
"""
================================================================================
Investigative Journalism Workflow Orchestrator (main.py)
================================================================================

Purpose:
  This script serves as the central control point for running an investigative
  journalism workflow. It automates the execution of various stages involved
  in data collection, processing, analysis, and reporting for a specific
  investigation case. The workflow is broken down into modular scripts,
  which this orchestrator calls in sequence or as specified by the user.

Core Functionality:
  - Manages different stages of an investigation (setup, acquisition,
    Datashare processing, parsing, analysis, packaging).
  - Takes a 'case_name' (corresponding to a directory in './investigations/')
    and one or more 'stages' to run as command-line arguments.
  - Allows for running the entire workflow or specific stages independently.
  - Provides options to control sub-tasks within certain stages (e.g.,
    acquiring specific document types, performing specific Datashare actions).
  - Logs all operations to both the console and a timestamped log file
    in the './logs' directory for traceability and debugging.
  - Includes basic timeout mechanisms for external script execution.

Prerequisites:
  - Python 3.7+
  - Dependencies listed in 'requirements.txt' must be installed
    (run: pip install -r requirements.txt).
  - For stages involving Datashare, a running Datashare instance is required,
    and its API endpoint should be configured (typically in './config/').
  - The project must follow the prescribed directory structure outlined in
    the project's README.md.

Basic Usage (from the project root directory):
  python main.py <case_name> --stage <stage_name>
  python main.py <case_name> --stage all

Examples:
  python main.py "MyFirstCase" --stage setup --stage acquire --acquire-type irs_990s
  python main.py "AnotherCase" --stage parse --parse-type corporate --script-timeout 3600
  python main.py "FullInvestigation" --stage all

For detailed setup, full usage instructions, and information on configuring
individual scripts (e.g., API keys, scraping targets), please refer to the
README.md file in the project root.
"""

import argparse
import subprocess
import os
import sys
import logging
import re
from datetime import datetime

# ------------------------------------------------------------------------------
# SCRIPT CONFIGURATION
# ------------------------------------------------------------------------------
# These variables define the core directory structure of the project.
# They are automatically determined based on the location of this script.
# It is generally NOT recommended to change these unless you have a
# highly customized project layout.
# Most operational configurations (API keys, specific URLs, parsing rules)
# should be managed in files within the './config/' directory.

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
# Assumes 'main.py' is in the project's root directory.

INVESTIGATIONS_DIR = os.path.join(PROJECT_ROOT, "investigations")
# Directory containing individual case folders.

SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
# Directory containing all the modular Python/shell scripts for each task.

CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
# Directory for configuration files (e.g., API settings, parser rules).

LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
# Directory where log files will be stored.

DEFAULT_SCRIPT_TIMEOUT = 7200 # Default timeout for sub-scripts in seconds (e.g., 2 hours)

# ------------------------------------------------------------------------------
# CUSTOM EXCEPTIONS
# ------------------------------------------------------------------------------
class OrchestratorError(Exception):
    """Base class for exceptions raised by this orchestrator."""
    pass

class SetupError(OrchestratorError):
    """Exception raised for errors during the setup stage."""
    pass

class StageError(OrchestratorError):
    """Exception raised for errors occurring within a specific workflow stage."""
    pass

class ScriptExecutionError(OrchestratorError):
    """Exception raised when a sub-script fails or times out."""
    pass

class InvalidCaseNameError(OrchestratorError):
    """Exception raised for invalid case names."""
    pass

# ------------------------------------------------------------------------------
# LOGGING SETUP
# ------------------------------------------------------------------------------
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
os.makedirs(LOGS_DIR, exist_ok=True) # Create logs directory if it doesn't exist
LOG_FILE = os.path.join(LOGS_DIR, f"orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO, # Set to logging.DEBUG for more verbose output
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)  # Also print to console
    ]
)
logging.info(f"Orchestrator script started. Logging to: {LOG_FILE}")
logging.info(f"Project Root: {PROJECT_ROOT}")


# --- Helper Functions ---
def validate_case_name(case_name):
    """
    Validates the case name to prevent path traversal and ensure it's a simple name.
    Allows alphanumeric characters, underscores, hyphens, and periods.
    """
    if not case_name:
        raise InvalidCaseNameError("Case name cannot be empty.")
    # Regex to allow alphanumeric, underscore, hyphen, period. Disallow path separators.
    if not re.match(r"^[a-zA-Z0-9_.-]+$", case_name):
        raise InvalidCaseNameError(
            f"Invalid case name: '{case_name}'. "
            "Allowed characters are alphanumeric, underscore, hyphen, period. "
            "Path separators (/, \\) are not allowed."
        )
    if ".." in case_name: # Double check for ".." just in case regex misses complex cases
        raise InvalidCaseNameError(f"Invalid case name: '{case_name}'. Path traversal '..' is not allowed.")
    return True


def validate_investigation_exists(case_name):
    """
    Checks if the specified investigation directory exists.
    An investigation directory is expected under INVESTIGATIONS_DIR.
    """
    try:
        validate_case_name(case_name)
    except InvalidCaseNameError as e:
        logging.error(str(e))
        return False

    investigation_path = os.path.join(INVESTIGATIONS_DIR, case_name)
    if not os.path.isdir(investigation_path):
        logging.error(f"Investigation directory not found: {investigation_path}")
        logging.error(f"Please ensure a directory named '{case_name}' exists within '{INVESTIGATIONS_DIR}' or run the 'setup' stage.")
        return False
    logging.debug(f"Validated investigation directory: {investigation_path}")
    return True

def run_script(script_path_relative, case_name, script_timeout, *args):
    """
    Runs a specified script located within the SCRIPTS_DIR using subprocess.
    Passes the case_name as the first argument to the target script,
    followed by any additional arguments provided in *args.

    Args:
        script_path_relative (str): The path to the script relative to SCRIPTS_DIR.
        case_name (str): The name of the current investigation case.
        script_timeout (int): Timeout in seconds for the script execution.
        *args: Additional arguments to pass to the script.

    Returns:
        bool: True if the script runs successfully (exit code 0), False otherwise.
    Raises:
        ScriptExecutionError: If the script is not found, times out, or fails.
    """
    full_script_path = os.path.join(SCRIPTS_DIR, script_path_relative)
    if not os.path.isfile(full_script_path):
        msg = f"Target script not found: {full_script_path}"
        logging.error(msg)
        raise ScriptExecutionError(msg)

    command = []
    if full_script_path.endswith(".py"):
        command = [sys.executable, full_script_path, case_name] + list(args)
    else:
        command = [full_script_path, case_name] + list(args)

    logging.info(f"Executing script: {' '.join(command)} (Timeout: {script_timeout}s)")
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=PROJECT_ROOT)
        stdout, stderr = process.communicate(timeout=script_timeout)

        if stdout:
            logging.info(f"Output from '{script_path_relative}' for case '{case_name}':\n{stdout.strip()}")
        if stderr:
            if process.returncode != 0:
                logging.error(f"Error output from '{script_path_relative}' for case '{case_name}' (return code {process.returncode}):\n{stderr.strip()}")
            else:
                logging.info(f"Stderr output from '{script_path_relative}' for case '{case_name}':\n{stderr.strip()}")

        if process.returncode != 0:
            msg = f"Script '{script_path_relative}' failed for case '{case_name}' with return code {process.returncode}."
            logging.error(msg)
            raise ScriptExecutionError(msg)

        logging.info(f"Script '{script_path_relative}' completed successfully for case '{case_name}'.")
        return True
    except FileNotFoundError as e:
        msg = f"Execution error: The script '{full_script_path}' was not found or there's an issue with the Python interpreter/path."
        logging.error(msg)
        raise ScriptExecutionError(msg) from e
    except subprocess.TimeoutExpired as e:
        process.kill() # Ensure the process is killed if it times out
        stdout, stderr = process.communicate() # Capture any final output
        logging.error(f"Timeout ({script_timeout}s) expired for script '{script_path_relative}' for case '{case_name}'. Process killed.")
        if stdout: logging.info(f"Final stdout before kill:\n{stdout.strip()}")
        if stderr: logging.error(f"Final stderr before kill:\n{stderr.strip()}")
        raise ScriptExecutionError(f"Script '{script_path_relative}' timed out for case '{case_name}'.") from e
    except Exception as e: # Catch other potential exceptions during subprocess execution
        msg = f"An unexpected error occurred while running '{script_path_relative}' for case '{case_name}': {type(e).__name__} - {e}"
        logging.error(msg)
        raise ScriptExecutionError(msg) from e

# --- Workflow Stages ---
# Each function defines a distinct stage in the investigative workflow.
# These functions orchestrate calls to the specialized, compartmentalized scripts
# located in the './scripts/' directory.
# FUTURE ENHANCEMENT: Workflow definitions (sequence of scripts, parameters)
# could be loaded from external configuration files (e.g., YAML) instead of
# being hardcoded in these stage functions, for greater flexibility.

def stage_0_setup_investigation(case_name, script_timeout):
    """
    Initial setup for a new investigation case.
    Validates case name, creates base directory and standard subdirectories.
    FUTURE: Could include initializing case-specific config files or pre-flight checks for tools.
    """
    logging.info(f"--- STAGE 0: SETUP INVESTIGATION for case: {case_name} ---")
    try:
        validate_case_name(case_name)
    except InvalidCaseNameError as e:
        logging.error(str(e))
        raise SetupError(str(e)) from e # Re-raise as SetupError

    investigation_path = os.path.join(INVESTIGATIONS_DIR, case_name)

    try:
        logging.info(f"Ensuring base directory exists for investigation '{case_name}': {investigation_path}")
        os.makedirs(investigation_path, exist_ok=True)
        logging.info(f"Base directory ensured: {investigation_path}")

        # Create standard sub-directory structure
        # FUTURE: This list could be made configurable.
        standard_subdirs = [
            "00_source_documents/irs_990s/pdfs", "00_source_documents/irs_990s/xmls",
            "00_source_documents/corporate_filings", "00_source_documents/property_records_raw",
            "00_source_documents/legal_documents", "00_source_documents/campaign_finance",
            "00_source_documents/lobbying_records", "00_source_documents/media_clippings_and_web_archives",
            "00_source_documents/other_public_records",
            "01_datashare_outputs/extracted_text", "01_datashare_outputs/ner_results",
            "02_parsed_and_structured_data/irs_990_parsed", "02_parsed_and_structured_data/corporate_parsed",
            "02_parsed_and_structured_data/property_parsed", "02_parsed_and_structured_data/campaign_finance_parsed",
            "02_parsed_and_structured_data/lobbying_parsed",
            "03_analysis_and_reports",
            "04_findings_and_narrative"
        ]
        for sub_dir_path_relative in standard_subdirs:
            # Handle potential nested subdirs in the path
            full_sub_dir_path = investigation_path
            for part in sub_dir_path_relative.split('/'):
                full_sub_dir_path = os.path.join(full_sub_dir_path, part)
            os.makedirs(full_sub_dir_path, exist_ok=True)
        logging.info(f"Standard subdirectories ensured within '{case_name}'.")

    except OSError as e:
        msg = f"Failed to create directories for case '{case_name}': {e}"
        logging.error(msg)
        raise SetupError(msg) from e

    # Placeholder for pre-flight checks or initializing case-specific config
    # e.g., run_script("utils/initialize_case_config.py", case_name, script_timeout)
    # e.g., run_script("utils/perform_preflight_checks.py", case_name, script_timeout)

    logging.info(f"Stage 0: Setup investigation for '{case_name}' completed.")
    return True


def stage_1_acquire_source_documents(case_name, source_type, script_timeout):
    """Orchestrates scripts for acquiring source documents."""
    logging.info(f"--- STAGE 1: ACQUIRE SOURCE DOCUMENTS for case: {case_name} (Type: {source_type or 'all'}) ---")
    if not validate_investigation_exists(case_name):
        raise StageError(f"Cannot run 'acquire' stage: Investigation directory for '{case_name}' not found or invalid.")

    overall_stage_success = True
    scripts_to_run = []
    # FUTURE: This mapping could be part of a configurable workflow definition.
    if source_type == 'irs_990s' or source_type == 'all' or source_type is None:
        scripts_to_run.append("scraping/fetch_irs_990_forms.py")
    # ... (add other acquisition script mappings)

    if not scripts_to_run:
        logging.warning(f"No acquisition scripts for source_type '{source_type}'. Stage may be incomplete.")
        return True # Not a failure if no scripts match the specific type

    for script_rel_path in scripts_to_run:
        try:
            run_script(script_rel_path, case_name, script_timeout)
        except ScriptExecutionError: # Specific error from run_script
            overall_stage_success = False
            # Error already logged by run_script
    return overall_stage_success

def stage_2_datashare_processing(case_name, action, script_timeout):
    """Manages interactions with a Datashare instance."""
    logging.info(f"--- STAGE 2: DATASHARE PROCESSING for case: {case_name} (Action: {action or 'all'}) ---")
    if not validate_investigation_exists(case_name):
        raise StageError(f"Cannot run 'datashare' stage: Investigation directory for '{case_name}' not found or invalid.")

    overall_stage_success = True
    actions_to_perform = []
    if action == 'create_project' or action == 'all' or action is None:
        actions_to_perform.append(("datashare_interactions/create_datashare_project.py", []))
    # ... (add other datashare action mappings)

    if not actions_to_perform:
        logging.warning(f"No Datashare actions for type '{action}'. Stage may be incomplete.")
        return True

    for script_rel_path, script_args in actions_to_perform:
        try:
            run_script(script_rel_path, case_name, script_timeout, *script_args)
        except ScriptExecutionError:
            overall_stage_success = False
    return overall_stage_success

def stage_3_parse_and_structure_data(case_name, document_type, script_timeout):
    """Executes parsing scripts to extract structured data."""
    logging.info(f"--- STAGE 3: PARSE AND STRUCTURE DATA for case: {case_name} (Type: {document_type or 'all'}) ---")
    if not validate_investigation_exists(case_name):
        raise StageError(f"Cannot run 'parse' stage: Investigation directory for '{case_name}' not found or invalid.")

    overall_stage_success = True
    scripts_to_run = []
    if document_type == 'irs_990s' or document_type == 'all' or document_type is None:
        scripts_to_run.append("parsers/parse_irs_990xml.py")
    # ... (add other parser script mappings)

    for script_rel_path in scripts_to_run:
        try:
            run_script(script_rel_path, case_name, script_timeout)
        except ScriptExecutionError:
            overall_stage_success = False

    if document_type == 'all' or document_type is None: # Entity resolution after parsing
        try:
            run_script("analysis/entity_resolution.py", case_name, script_timeout)
        except ScriptExecutionError:
            overall_stage_success = False
    return overall_stage_success

def stage_4_analysis_and_reporting(case_name, report_type, script_timeout):
    """Runs analysis scripts and generates reports."""
    logging.info(f"--- STAGE 4: ANALYSIS AND REPORTING for case: {case_name} (Report: {report_type or 'all'}) ---")
    if not validate_investigation_exists(case_name):
        raise StageError(f"Cannot run 'analyze' stage: Investigation directory for '{case_name}' not found or invalid.")

    overall_stage_success = True
    scripts_to_run = []
    if report_type == 'connections' or report_type == 'all' or report_type is None:
        scripts_to_run.append("analysis/generate_connections_report.py")
    # ... (add other analysis script mappings)

    for script_rel_path in scripts_to_run:
        try:
            run_script(script_rel_path, case_name, script_timeout)
        except ScriptExecutionError:
            overall_stage_success = False
    return overall_stage_success

def stage_5_package_for_review(case_name, script_timeout):
    """Placeholder for final stage tasks."""
    logging.info(f"--- STAGE 5: PACKAGE FOR REVIEW for case: {case_name} ---")
    if not validate_investigation_exists(case_name):
        raise StageError(f"Cannot run 'package' stage: Investigation directory for '{case_name}' not found or invalid.")
    # Example:
    # try:
    #     run_script("utils/package_case_findings.py", case_name, script_timeout)
    # except ScriptExecutionError:
    #     return False
    logging.info(f"Stage 5: Package for review for '{case_name}' completed (placeholder).")
    return True


# --- Main Execution Logic ---
def main():
    """Main function to parse command-line arguments and orchestrate the workflow stages."""
    parser = argparse.ArgumentParser(
        description="Orchestrator for the Investigative Journalism Workflow. See comments at the top of this script and README.md for more details.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "case_name",
        help="The name of the investigation case. This must correspond to a directory\n"
             "name within the './investigations/' directory (e.g., 'MyFirstCase')."
    )
    parser.add_argument(
        "--stage", action="append",
        choices=['setup', 'acquire', 'datashare', 'parse', 'analyze', 'package', 'all'],
        required=True,
        help="Which stage(s) of the workflow to run. Specify multiple times for multiple stages\n"
             "(e.g., --stage acquire --stage parse). 'all' runs all defined stages sequentially."
    )
    parser.add_argument(
        "--script-timeout", type=int, default=DEFAULT_SCRIPT_TIMEOUT,
        help=f"Global timeout in seconds for individual scripts called by the orchestrator (default: {DEFAULT_SCRIPT_TIMEOUT}s)."
    )
    # Optional arguments for stages
    parser.add_argument("--acquire-type", choices=['irs_990s', 'corporate', 'campaign', 'lobbying', 'property', 'all', None], default=None, help="For 'acquire' stage: specific document type (default: all).")
    parser.add_argument("--datashare-action", choices=['create_project', 'upload', 'query_entities', 'all', None], default=None, help="For 'datashare' stage: specific action (default: all).")
    parser.add_argument("--parse-type", choices=['irs_990s', 'corporate', 'campaign', 'lobbying', 'all', None], default=None, help="For 'parse' stage: specific document type (default: all).")
    parser.add_argument("--report-type", choices=['connections', 'network_graph', 'financial_patterns', 'all', None], default=None, help="For 'analyze' stage: specific report type (default: all).")

    args = parser.parse_args()
    case_name = args.case_name
    stages_to_run_input = args.stage
    script_timeout = args.script_timeout

    logging.info(f"================================================================================")
    logging.info(f"Initializing workflow for investigation case: '{case_name}'")
    logging.info(f"Requested stage(s): {', '.join(stages_to_run_input)}")
    logging.info(f"Global script timeout set to: {script_timeout} seconds")
    logging.info(f"================================================================================")

    try:
        validate_case_name(case_name) # Initial validation of case_name format
    except InvalidCaseNameError as e:
        logging.error(f"Workflow halted due to invalid case name: {e}")
        sys.exit(1)


    if 'all' in stages_to_run_input:
        stages_to_run_ordered = ['setup', 'acquire', 'datashare', 'parse', 'analyze', 'package']
        logging.info("Processing 'all' stages in predefined order.")
    else:
        stages_to_run_ordered = stages_to_run_input

    overall_workflow_success = True
    for stage_name in stages_to_run_ordered:
        current_stage_success = False
        try:
            if not overall_workflow_success and stage_name not in ['setup']:
                logging.warning(f"Skipping stage '{stage_name}' for case '{case_name}' due to failure in a preceding stage.")
                continue

            logging.info(f"--- Starting Stage: {stage_name.upper()} for case '{case_name}' ---")

            if stage_name == 'setup':
                current_stage_success = stage_0_setup_investigation(case_name, script_timeout)
            elif stage_name == 'acquire':
                current_stage_success = stage_1_acquire_source_documents(case_name, args.acquire_type, script_timeout)
            elif stage_name == 'datashare':
                current_stage_success = stage_2_datashare_processing(case_name, args.datashare_action, script_timeout)
            elif stage_name == 'parse':
                current_stage_success = stage_3_parse_and_structure_data(case_name, args.parse_type, script_timeout)
            elif stage_name == 'analyze':
                current_stage_success = stage_4_analysis_and_reporting(case_name, args.report_type, script_timeout)
            elif stage_name == 'package':
                current_stage_success = stage_5_package_for_review(case_name, script_timeout)
            else:
                logging.error(f"Encountered an unknown stage: '{stage_name}'.")
                # This should not happen if argparse choices are correct
                raise OrchestratorError(f"Unknown stage definition: {stage_name}")

            if not current_stage_success: # If a stage function returns False explicitly
                raise StageError(f"Stage '{stage_name}' reported failure for case '{case_name}'.")

            logging.info(f"--- Stage: {stage_name.upper()} for case '{case_name}' COMPLETED successfully ---")

        except (OrchestratorError, SetupError, StageError, ScriptExecutionError) as e: # Catch our custom errors
            logging.error(f"!!! Stage '{stage_name}' FAILED for case '{case_name}' with error: {type(e).__name__} - {e} !!!")
            overall_workflow_success = False
            # Depending on severity, might want to break or offer options to continue non-dependent stages.
            # For now, we mark overall failure and continue to log other requested stages as skipped.
        except Exception as e: # Catch any other unexpected errors
            logging.critical(f"!!! UNEXPECTED CRITICAL ERROR during stage '{stage_name}' for case '{case_name}': {type(e).__name__} - {e} !!!", exc_info=True)
            overall_workflow_success = False
            # For truly unexpected errors, it's often best to halt.

    logging.info(f"================================================================================")
    if overall_workflow_success:
        logging.info(f"Workflow for investigation case '{case_name}' concluded successfully.")
    else:
        logging.error(f"Workflow for investigation case '{case_name}' concluded with one or more FAILED stages.")
        logging.error("Please review the log file for detailed error messages: " + LOG_FILE)
        sys.exit(1)
    logging.info(f"================================================================================")

if __name__ == "__main__":
    main()
