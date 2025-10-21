#!/bin/bash

# ====================================================================
# Backend Test Automation Runner for Crypto Market Monitoring System
# ====================================================================
#
# This script provides comprehensive test automation for the cryptocurrency
# market monitoring backend system with multiple execution modes:
#
# Usage:
#   ./run_tests.sh [quick|full|performance|bug-validation|ci]
#
# Test Categories:
#   - Unit Tests: Fast, isolated component tests
#   - Integration Tests: Multi-component interaction tests
#   - Performance Tests: Load and response time validation
#   - Bug Validation: Specific bug fix verification
#   - CI/CD: Continuous integration test suite

set -euo pipefail  # Exit on any error, undefined vars, pipe failures

# Color codes for output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
TEST_ROOT="${SCRIPT_DIR}"
REPORT_DIR="${TEST_ROOT}/reports"
LOG_DIR="${TEST_ROOT}/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Test execution mode (default: quick)
TEST_MODE="${1:-quick}"

# Test environment configuration
export PYTHONPATH="${PROJECT_ROOT}/data-scripts:${PROJECT_ROOT}/data-streams:${PYTHONPATH:-}"
export TEST_ENVIRONMENT="true"
export DISABLE_EXTERNAL_APIS="true"  # Prevent actual API calls during testing

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_header() {
    echo ""
    echo -e "${CYAN}=====================================================================${NC}"
    echo -e "${CYAN} $1${NC}"
    echo -e "${CYAN}=====================================================================${NC}"
    echo ""
}

# Environment setup
setup_test_environment() {
    log_header "Setting Up Test Environment"

    # Create necessary directories
    mkdir -p "${REPORT_DIR}" "${LOG_DIR}"

    # Check if virtual environment exists
    if [[ ! -d "${PROJECT_ROOT}/venv" ]]; then
        log_warning "Virtual environment not found. Creating one..."
        python3 -m venv "${PROJECT_ROOT}/venv"
    fi

    # Activate virtual environment
    source "${PROJECT_ROOT}/venv/bin/activate" || {
        log_error "Failed to activate virtual environment"
        exit 1
    }

    # Install test dependencies
    log_info "Installing test dependencies..."
    pip install -q -r "${TEST_ROOT}/requirements-test.txt" || {
        log_error "Failed to install test dependencies"
        exit 1
    }

    # Verify pytest is available
    if ! command -v pytest >/dev/null 2>&1; then
        log_error "pytest is not available. Please check installation."
        exit 1
    fi

    log_success "Test environment setup complete"
}

# Pre-test validation
pre_test_validation() {
    log_header "Pre-Test System Validation"

    # Check if main system files exist
    local required_files=(
        "${PROJECT_ROOT}/data-scripts/unified_ohlcv_collector.py"
        "${PROJECT_ROOT}/data-streams/cmc_real_time_monitor.py"
    )

    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            log_error "Required file not found: $file"
            exit 1
        fi
    done

    # Validate Python syntax for main modules
    log_info "Validating Python syntax for main modules..."
    python -m py_compile "${PROJECT_ROOT}/data-scripts/unified_ohlcv_collector.py" || {
        log_error "Syntax error in unified_ohlcv_collector.py"
        exit 1
    }

    python -m py_compile "${PROJECT_ROOT}/data-streams/cmc_real_time_monitor.py" || {
        log_error "Syntax error in cmc_real_time_monitor.py"
        exit 1
    }

    log_success "Pre-test validation complete"
}

# Test execution functions
run_unit_tests() {
    log_header "Running Unit Tests"

    local test_args=(
        "-v"
        "--tb=short"
        "--color=yes"
        "-m" "unit or not integration and not performance and not slow"
        "--junit-xml=${REPORT_DIR}/unit_tests_${TIMESTAMP}.xml"
        "--html=${REPORT_DIR}/unit_tests_${TIMESTAMP}.html"
        "--self-contained-html"
    )

    if pytest "${test_args[@]}" "${TEST_ROOT}"; then
        log_success "Unit tests passed"
        return 0
    else
        log_error "Unit tests failed"
        return 1
    fi
}

run_integration_tests() {
    log_header "Running Integration Tests"

    local test_args=(
        "-v"
        "--tb=line"
        "--color=yes"
        "-m" "integration"
        "--junit-xml=${REPORT_DIR}/integration_tests_${TIMESTAMP}.xml"
        "--html=${REPORT_DIR}/integration_tests_${TIMESTAMP}.html"
        "--self-contained-html"
    )

    if pytest "${test_args[@]}" "${TEST_ROOT}"; then
        log_success "Integration tests passed"
        return 0
    else
        log_error "Integration tests failed"
        return 1
    fi
}

run_performance_tests() {
    log_header "Running Performance Tests"

    local test_args=(
        "-v"
        "--tb=short"
        "--color=yes"
        "-m" "performance"
        "--junit-xml=${REPORT_DIR}/performance_tests_${TIMESTAMP}.xml"
        "--html=${REPORT_DIR}/performance_tests_${TIMESTAMP}.html"
        "--self-contained-html"
        "--benchmark-only"
        "--benchmark-json=${REPORT_DIR}/benchmark_${TIMESTAMP}.json"
    )

    if pytest "${test_args[@]}" "${TEST_ROOT}"; then
        log_success "Performance tests passed"
        return 0
    else
        log_error "Performance tests failed"
        return 1
    fi
}

run_bug_validation_tests() {
    log_header "Running Bug Validation Tests"

    local test_args=(
        "-v"
        "--tb=long"
        "--color=yes"
        "-m" "bug_validation"
        "--junit-xml=${REPORT_DIR}/bug_validation_${TIMESTAMP}.xml"
        "--html=${REPORT_DIR}/bug_validation_${TIMESTAMP}.html"
        "--self-contained-html"
    )

    # Run specific market cap bug validation
    log_info "Testing market cap calculation bug fix..."
    if pytest "${test_args[@]}" "${TEST_ROOT}/test_market_cap_bug_fix.py"; then
        log_success "Market cap bug validation passed"
    else
        log_error "Market cap bug validation failed"
        return 1
    fi

    # Run all other bug validation tests
    if pytest "${test_args[@]}" "${TEST_ROOT}"; then
        log_success "All bug validation tests passed"
        return 0
    else
        log_error "Bug validation tests failed"
        return 1
    fi
}

run_data_quality_tests() {
    log_header "Running Data Quality Tests"

    local test_args=(
        "-v"
        "--tb=short"
        "--color=yes"
        "-m" "data_quality"
        "--junit-xml=${REPORT_DIR}/data_quality_${TIMESTAMP}.xml"
        "--html=${REPORT_DIR}/data_quality_${TIMESTAMP}.html"
        "--self-contained-html"
    )

    if pytest "${test_args[@]}" "${TEST_ROOT}"; then
        log_success "Data quality tests passed"
        return 0
    else
        log_error "Data quality tests failed"
        return 1
    fi
}

run_error_handling_tests() {
    log_header "Running Error Handling & Resilience Tests"

    local test_args=(
        "-v"
        "--tb=short"
        "--color=yes"
        "-m" "error_handling"
        "--junit-xml=${REPORT_DIR}/error_handling_${TIMESTAMP}.xml"
        "--html=${REPORT_DIR}/error_handling_${TIMESTAMP}.html"
        "--self-contained-html"
    )

    if pytest "${test_args[@]}" "${TEST_ROOT}"; then
        log_success "Error handling tests passed"
        return 0
    else
        log_error "Error handling tests failed"
        return 1
    fi
}

# Test coverage analysis
generate_coverage_report() {
    log_header "Generating Test Coverage Report"

    local coverage_args=(
        "--cov=${PROJECT_ROOT}/data-scripts"
        "--cov=${PROJECT_ROOT}/data-streams"
        "--cov-report=html:${REPORT_DIR}/coverage_${TIMESTAMP}"
        "--cov-report=xml:${REPORT_DIR}/coverage_${TIMESTAMP}.xml"
        "--cov-report=term-missing"
        "--cov-branch"
    )

    pytest "${coverage_args[@]}" "${TEST_ROOT}" || {
        log_warning "Coverage report generation encountered issues"
    }

    log_success "Coverage report generated in ${REPORT_DIR}/coverage_${TIMESTAMP}"
}

# Test result analysis
analyze_test_results() {
    log_header "Analyzing Test Results"

    local total_tests=0
    local passed_tests=0
    local failed_tests=0

    # Count test results from XML reports
    if command -v xmllint >/dev/null 2>&1; then
        for xml_file in "${REPORT_DIR}"/*_"${TIMESTAMP}".xml; do
            if [[ -f "$xml_file" ]]; then
                local file_tests=$(xmllint --xpath "string(/testsuite/@tests)" "$xml_file" 2>/dev/null || echo "0")
                local file_failures=$(xmllint --xpath "string(/testsuite/@failures)" "$xml_file" 2>/dev/null || echo "0")
                local file_errors=$(xmllint --xpath "string(/testsuite/@errors)" "$xml_file" 2>/dev/null || echo "0")

                total_tests=$((total_tests + file_tests))
                failed_tests=$((failed_tests + file_failures + file_errors))
            fi
        done

        passed_tests=$((total_tests - failed_tests))
    fi

    # Generate summary report
    cat > "${REPORT_DIR}/test_summary_${TIMESTAMP}.txt" << EOF
Cryptocurrency Market Monitoring Backend Test Summary
====================================================

Execution Time: $(date)
Test Mode: ${TEST_MODE}
Total Tests: ${total_tests}
Passed: ${passed_tests}
Failed: ${failed_tests}

Test Categories Executed:
EOF

    case "${TEST_MODE}" in
        "quick")
            echo "- Unit Tests" >> "${REPORT_DIR}/test_summary_${TIMESTAMP}.txt"
            ;;
        "full")
            echo "- Unit Tests" >> "${REPORT_DIR}/test_summary_${TIMESTAMP}.txt"
            echo "- Integration Tests" >> "${REPORT_DIR}/test_summary_${TIMESTAMP}.txt"
            echo "- Performance Tests" >> "${REPORT_DIR}/test_summary_${TIMESTAMP}.txt"
            echo "- Bug Validation Tests" >> "${REPORT_DIR}/test_summary_${TIMESTAMP}.txt"
            ;;
        "ci")
            echo "- Unit Tests" >> "${REPORT_DIR}/test_summary_${TIMESTAMP}.txt"
            echo "- Integration Tests" >> "${REPORT_DIR}/test_summary_${TIMESTAMP}.txt"
            echo "- Bug Validation Tests" >> "${REPORT_DIR}/test_summary_${TIMESTAMP}.txt"
            ;;
    esac

    # Display summary
    log_info "Test execution summary:"
    echo "  Total Tests: ${total_tests}"
    echo "  Passed: ${GREEN}${passed_tests}${NC}"
    echo "  Failed: ${RED}${failed_tests}${NC}"

    if [[ $failed_tests -eq 0 ]]; then
        log_success "All tests passed!"
        return 0
    else
        log_error "${failed_tests} tests failed"
        return 1
    fi
}

# Main execution logic
main() {
    log_header "Cryptocurrency Market Monitoring Backend Test Runner"

    local start_time=$(date +%s)
    local exit_code=0

    # Setup
    setup_test_environment
    pre_test_validation

    # Execute tests based on mode
    case "${TEST_MODE}" in
        "quick")
            log_info "Running quick test suite (unit tests only)"
            run_unit_tests || exit_code=1
            ;;

        "integration")
            log_info "Running integration test suite"
            run_integration_tests || exit_code=1
            ;;

        "performance")
            log_info "Running performance test suite"
            run_performance_tests || exit_code=1
            ;;

        "bug-validation")
            log_info "Running bug validation test suite"
            run_bug_validation_tests || exit_code=1
            ;;

        "data-quality")
            log_info "Running data quality test suite"
            run_data_quality_tests || exit_code=1
            ;;

        "error-handling")
            log_info "Running error handling test suite"
            run_error_handling_tests || exit_code=1
            ;;

        "full")
            log_info "Running full comprehensive test suite"
            run_unit_tests || exit_code=1
            run_integration_tests || exit_code=1
            run_performance_tests || exit_code=1
            run_bug_validation_tests || exit_code=1
            run_data_quality_tests || exit_code=1
            run_error_handling_tests || exit_code=1
            ;;

        "ci")
            log_info "Running CI/CD test suite"
            run_unit_tests || exit_code=1
            run_integration_tests || exit_code=1
            run_bug_validation_tests || exit_code=1
            ;;

        *)
            log_error "Invalid test mode: ${TEST_MODE}"
            echo "Valid modes: quick, integration, performance, bug-validation, data-quality, error-handling, full, ci"
            exit 1
            ;;
    esac

    # Generate coverage report for comprehensive test runs
    if [[ "${TEST_MODE}" == "full" || "${TEST_MODE}" == "ci" ]]; then
        generate_coverage_report
    fi

    # Analyze and report results
    analyze_test_results || exit_code=1

    # Calculate execution time
    local end_time=$(date +%s)
    local execution_time=$((end_time - start_time))

    log_info "Test execution completed in ${execution_time} seconds"

    # Final status
    if [[ $exit_code -eq 0 ]]; then
        log_success "All tests completed successfully!"
        log_info "Reports available in: ${REPORT_DIR}"
    else
        log_error "Some tests failed. Check reports for details."
        log_info "Reports available in: ${REPORT_DIR}"
    fi

    exit $exit_code
}

# Help function
show_help() {
    cat << EOF
Cryptocurrency Market Monitoring Backend Test Runner

USAGE:
    $0 [MODE]

MODES:
    quick           Run unit tests only (default, fastest)
    integration     Run integration tests
    performance     Run performance and load tests
    bug-validation  Run bug fix validation tests
    data-quality    Run data validation and quality tests
    error-handling  Run error handling and resilience tests
    full            Run all test categories (comprehensive)
    ci              Run CI/CD test suite (unit + integration + bug validation)

EXAMPLES:
    $0                      # Quick unit tests
    $0 full                 # Complete test suite
    $0 bug-validation       # Market cap bug validation
    $0 ci                   # CI/CD pipeline tests

OUTPUT:
    Test reports are generated in: tests/backend/reports/
    HTML reports, XML results, and coverage data available after execution.

ENVIRONMENT:
    - Requires Python 3.8+ with virtual environment
    - Test dependencies installed from requirements-test.txt
    - Project modules available in PYTHONPATH
EOF
}

# Handle help request
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    show_help
    exit 0
fi

# Execute main function
main "$@"