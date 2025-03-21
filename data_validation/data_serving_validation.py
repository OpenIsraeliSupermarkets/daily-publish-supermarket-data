from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from dataclasses import dataclass
from enum import Enum
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import os
import json

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
        """Fetch and validate all chains, files, and their content"""
        try:
            self.set_loading(True)
            self.set_error(None)
            
            # Fetch all chains
            chains = await self.fetch_chains()
            self.set_chains(chains)
            
            results = {}
            
            # Process each chain
            for chain in chains:
                print(f"Processing chain {chain}")
                results[chain] = {
                    "files": [],
                    "validation_results": []
                }
                
                # Fetch files for the chain
                files = await self.fetch_files_for_chain(chain)
                results[chain]["files"] = files
                
                # Process each file
                for file in files:
                    print(f"Processing file {file}")
                    try:
                        # Fetch file content
                        content = await self.fetch_file_content(chain, file)
                        
                        # Validate the content                        
                        results[chain]["validation_results"].append({
                            "file": file,
                            "validation": {
                                 "num_of_rows": len(content),
                                 "api_call_status": "success"
                            }
                        })
                        
                    except Exception as e:
                        logging.error(f"Error processing file {file} for chain {chain}: {str(e)}")
                        results[chain]["validation_results"].append({
                            "file": file,
                            "validation":{
                                "api_call_status": "error",
                                "error": str(e)
                            }
                            
                        })
                    
            return results
            
        except Exception as e:
            self.set_error(str(e))
            logging.error(f"Error in validate_all_data: {str(e)}")
            raise
        finally:
            self.set_loading(False)

    def set_loading(self, loading: bool) -> None:
        self.loading = loading

    def set_error(self, error: Optional[str]) -> None:
        self.error = error

    def set_chains(self, chains: List[str]) -> None:
        self.chains = chains

    def set_selected_chain(self, chain: Optional[str]) -> None:
        self.selected_chain = chain
        self.files = []
        self.selected_file = None
        self.file_content = None

    def set_files(self, files: List[str]) -> None:
        self.files = files

    def set_selected_file(self, file: Optional[str]) -> None:
        self.selected_file = file
        self.file_content = None

    def set_file_content(self, content: Optional[Dict[str, Any]]) -> None:
        self.file_content = content

    def request_sort(self, key: str) -> None:
        self.sort_config = {
            "key": key,
            "direction": "descending" if self.sort_config["key"] == key and self.sort_config["direction"] == "ascending" else "ascending"
        }

    def get_sorted_and_filtered_data(self) -> List[Dict[str, Any]]:
        if not self.file_content or "rows" not in self.file_content:
            return []

        filtered_data = self.file_content["rows"]

        if self.sort_config["key"]:
            filtered_data.sort(
                key=lambda x: x["row_content"][self.sort_config["key"]],
                reverse=self.sort_config["direction"] == "descending"
            )

        return filtered_data



    def get_validation_summary(self) -> Dict[str, Any]:
        if not self.file_content:
            return {}

        total_rows = len(self.file_content.get("rows", []))
        successful_rows = sum(1 for row in self.file_content.get("rows", []) 
                            if row.get("row_content", {}).get("status") == "success")
        
        return {
            "total_rows": total_rows,
            "successful_rows": successful_rows,
            "failed_rows": total_rows - successful_rows,
            "success_rate": (successful_rows / total_rows * 100) if total_rows > 0 else 0
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

