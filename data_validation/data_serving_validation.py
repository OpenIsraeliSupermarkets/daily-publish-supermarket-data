from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from dataclasses import dataclass
from enum import Enum
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import os
import json
import multiprocessing
from functools import partial

@dataclass
class ValidationResult:
    num_of_rows: int
    status: str

class ApiCallValidator:
    def __init__(self, api_token: str, host: str = "http://localhost:8080"):
        self.validation_results: Dict[str, Dict[str, ValidationResult]] = {}
        self.loading = False
        self.error: Optional[str] = None
        self.chains: List[str] = []
        self.selected_chain: Optional[str] = None
        self.files: List[str] = []
        self.selected_file: Optional[str] = None
        self.file_content: Optional[Dict[str, Any]] = None
        self.sort_config = {
            "key": "",
            "direction": "ascending"
        }
        self.api_token = api_token
        self.host = host.rstrip('/')  # Remove trailing slash if present
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}"
        }

    async def fetch_chains(self) -> List[str]:
        """Fetch all available chains from the API"""
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(f"{self.host}/list_chains") as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch chains: {response.status}")
                data = await response.json()
                return data.get("list_of_chains", [])

    async def fetch_files_for_chain(self, chain: str) -> List[str]:
        """Fetch all files for a specific chain"""
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(f"{self.host}/list_scraped_files?chain={chain}") as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch files for chain {chain}: {response.status}")
                data = await response.json()
                return [file["file_name"] for file in data.get("processed_files", [])]

    async def fetch_file_content(self, chain: str, file: str) -> Dict[str, Any]:
        """Fetch content for a specific file"""
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(
                f"{self.host}/raw/file_content?chain={chain}&file={file}"
            ) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch content for {chain}/{file}: {response.status}")
                return (await response.json()).get("rows", [])

    async def validate_all_data(self) -> Dict[str, Dict[str, Any]]:
        """Fetch and validate all chains, files, and their content using multiprocessing"""
        try:
            
            # Fetch all chains
            chains = await self.fetch_chains()            
            # Create a process pool
            num_processes = 3
            with ProcessPoolExecutor(max_workers=num_processes) as executor:
                # Process chains in parallel
                chain_tasks = []
                for chain in chains:
                    chain_tasks.append(
                        asyncio.create_task(self._process_chain(chain, executor))
                    )
                
                # Wait for all chain processing to complete
                chain_results = await asyncio.gather(*chain_tasks)
                
                # Combine results
                results = {chain: result for chain, result in zip(chains, chain_results)}
            
            return results
            
        except Exception as e:
            logging.error(f"Error in validate_all_data: {str(e)}")
            raise


    async def _process_chain(self, chain: str, executor: ProcessPoolExecutor) -> Dict[str, Any]:
        """Process a single chain and its files"""
        print(f"Processing chain {chain}")
        result = {
            "files": [],
            "validation_results": []
        }
        
        try:
            # Fetch files for the chain
            files = await self.fetch_files_for_chain(chain)
            result["files"] = files
            
            # Process files in parallel using the process pool
            file_tasks = []
            for file in files:
                file_tasks.append(
                    asyncio.create_task(self._process_file(chain, file, executor))
                )
            
            # Wait for all file processing to complete
            file_results = await asyncio.gather(*file_tasks)
            result["validation_results"] = file_results
            
        except Exception as e:
            logging.error(f"Error processing chain {chain}: {str(e)}")
            result["validation_results"] = [{
                "file": "chain_error",
                "validation": {
                    "api_call_status": "error",
                    "error": str(e)
                }
            }]
        
        return result

    async def _process_file(self, chain: str, file: str, executor: ProcessPoolExecutor) -> Dict[str, Any]:
        """Process a single file"""
        print(f"Processing file {file}")
        try:
            # Fetch file content
            content = await self.fetch_file_content(chain, file)
            
            # Validate the content in a separate process
            loop = asyncio.get_event_loop()
            validation_result = await loop.run_in_executor(
                executor,
                self._validate_content,
                content
            )
            
            return {
                "file": file,
                "validation": validation_result
            }
            
        except Exception as e:
            logging.error(f"Error processing file {file} for chain {chain}: {str(e)}")
            return {
                "file": file,
                "validation": {
                    "api_call_status": "error",
                    "error": str(e)
                }
            }

    def _validate_content(self, content: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate content in a separate process"""
        return {
            "num_of_rows": len(content),
            "api_call_status": "success"
        }


async def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Get API token and host from environment variables
    api_token = os.getenv("API_TOKEN","0f8698d8-db8f-46e7-b460-8e0a2f3abab9")
    host = os.getenv("API_HOST", "http://192.168.1.129:8080")  # Default to localhost if not set
    
    if not api_token:
        raise ValueError("API_TOKEN environment variable is not set")
    
    logging.info(f"Using API host: {host}")
    
    # Initialize validator with host
    validator = ApiCallValidator(api_token, host)
    
    try:
        logging.info("Starting data validation process...")
        
        # Run validation
        results = await validator.validate_all_data()
        
        # Save results to file
        output_file = "validation_results.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logging.info(f"\nResults saved to {output_file}")
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        raise
    finally:
        logging.info("Validation process completed")

if __name__ == "__main__":
    asyncio.run(main())

