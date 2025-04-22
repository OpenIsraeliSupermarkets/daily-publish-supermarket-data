import asyncio
import logging
import traceback
import json
import importlib.util
import sys
import os

def check_module_available(module_name):
    """Check if a module is available without importing it directly."""
    return importlib.util.find_spec(module_name) is not None

async def run_validations():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    results = {}
    
    # Run data processing validation
    if check_module_available("pymongo") and check_module_available("il_supermarket_scarper"):
        try:
            logging.info("Starting data processing validation...")
            from data_processing_validation import collect_validation_results
            validation_results, aggregated_errors = collect_validation_results()
            
            # Save data processing validation results
            with open("processing_validation_results.json", "w") as f:
                json.dump(validation_results, f, indent=4)
            with open("processing_aggregated_errors.json", "w") as f:
                json.dump(aggregated_errors, f, indent=4)
            logging.info("Data processing validation completed")
            results["processing_validation"] = "completed"
        except Exception as e:
            logging.error(f"Error in data processing validation: {str(e)}")
            logging.error(traceback.format_exc())
            results["processing_validation"] = f"failed: {str(e)}"
    else:
        logging.warning("Skipping data processing validation - required modules not available")
        results["processing_validation"] = "skipped (missing dependencies)"
    
    # Run data serving validation
    try:
        logging.info("Starting data serving validation...")
        from data_serving_validation import ApiCallValidator
        
        api_token = os.getenv("API_TOKEN", "0f8698d8-db8f-46e7-b460-8e0a2f3abab9")
        host = os.getenv("API_HOST", "http://192.168.1.129:8080")
        rate_limit = int(os.getenv("RATE_LIMIT", "3"))
        
        validator = ApiCallValidator(api_token, host, rate_limit)
        serving_results = await validator.validate_all_data()
        
        # Save data serving validation results
        with open("serving_validation_results.json", "w") as f:
            json.dump(serving_results, f, indent=2, default=str)
        logging.info("Data serving validation completed")
        results["serving_validation"] = "completed"
    except Exception as e:
        logging.error(f"Error in data serving validation: {str(e)}")
        logging.error(traceback.format_exc())
        results["serving_validation"] = f"failed: {str(e)}"
    
    return results

if __name__ == "__main__":
    result = asyncio.run(run_validations())
    print("Validation Results:")
    for validation, status in result.items():
        print(f"- {validation}: {status}")
